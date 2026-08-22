//! Vector storage codecs shared by ingestion, mmap scans, and Arrow projection.
//!
//! Every codec has a fixed row width for a given dimension.  That property is
//! important for V4 row groups: a row can be addressed without an offset table
//! and TopK scans can score the compressed bytes directly.

use std::io;
use std::{cmp::Ordering, collections::BinaryHeap};

pub const TURBOQUANT_CODEC_VERSION: u8 = 1;
pub const TURBOQUANT_SEED: u64 = 0x4150_4558_5451_0001;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VectorCodec {
    Float32,
    Float16,
    BFloat16,
    Int8,
    UInt8,
    Bit1,
    TurboQuant2,
    TurboQuant3,
    TurboQuant4,
}

impl VectorCodec {
    #[inline]
    pub const fn bits(self) -> Option<u8> {
        match self {
            Self::TurboQuant2 => Some(2),
            Self::TurboQuant3 => Some(3),
            Self::TurboQuant4 => Some(4),
            _ => None,
        }
    }

    pub fn row_width(self, dim: usize) -> io::Result<usize> {
        let width = match self {
            Self::Float32 => dim.checked_mul(4),
            Self::Float16 | Self::BFloat16 => dim.checked_mul(2),
            Self::Int8 => dim.checked_add(4),
            Self::UInt8 => dim.checked_add(8),
            Self::Bit1 => dim.div_ceil(8).checked_add(4),
            Self::TurboQuant2 | Self::TurboQuant3 | Self::TurboQuant4 => {
                let padded = turbo_padded_dim(dim)?;
                packed_len(padded, self.bits().unwrap()).checked_add(4)
            }
        };
        width.ok_or_else(|| invalid_data("vector row width overflow"))
    }
}

#[inline]
pub fn f32_to_bf16(value: f32) -> u16 {
    let bits = value.to_bits();
    let rounding_bias = 0x7fff + ((bits >> 16) & 1);
    (bits.wrapping_add(rounding_bias) >> 16) as u16
}

#[inline]
pub fn bf16_to_f32(bits: u16) -> f32 {
    f32::from_bits((bits as u32) << 16)
}

pub fn encode_vector(codec: VectorCodec, values: &[f32], out: &mut Vec<u8>) -> io::Result<()> {
    if values.is_empty() {
        return Err(invalid_input("vector dimension must be greater than zero"));
    }
    if values.iter().any(|value| !value.is_finite()) {
        return Err(invalid_input("vector values must be finite"));
    }
    out.reserve(codec.row_width(values.len())?);
    match codec {
        VectorCodec::Float32 => {
            for &value in values {
                out.extend_from_slice(&value.to_le_bytes());
            }
        }
        VectorCodec::Float16 => {
            for &value in values {
                out.extend_from_slice(
                    &crate::storage::on_demand::f32_to_f16(value).to_le_bytes(),
                );
            }
        }
        VectorCodec::BFloat16 => {
            for &value in values {
                out.extend_from_slice(&f32_to_bf16(value).to_le_bytes());
            }
        }
        VectorCodec::Int8 => encode_int8(values, out),
        VectorCodec::UInt8 => encode_uint8(values, out),
        VectorCodec::Bit1 => encode_bit1(values, out),
        VectorCodec::TurboQuant2 | VectorCodec::TurboQuant3 | VectorCodec::TurboQuant4 => {
            encode_turboquant(values, codec.bits().unwrap(), out)?
        }
    }
    Ok(())
}

pub fn decode_vector(codec: VectorCodec, row: &[u8], dim: usize, out: &mut [f32]) -> io::Result<()> {
    if dim == 0 || out.len() != dim {
        return Err(invalid_input("invalid vector decode dimension"));
    }
    let expected = codec.row_width(dim)?;
    if row.len() != expected {
        return Err(invalid_data(format!(
            "vector row has {} bytes, expected {expected}",
            row.len()
        )));
    }
    match codec {
        VectorCodec::Float32 => {
            for (dst, bytes) in out.iter_mut().zip(row.chunks_exact(4)) {
                *dst = f32::from_le_bytes(bytes.try_into().unwrap());
            }
        }
        VectorCodec::Float16 => {
            for (dst, bytes) in out.iter_mut().zip(row.chunks_exact(2)) {
                *dst = crate::storage::on_demand::f16_to_f32(u16::from_le_bytes(
                    bytes.try_into().unwrap(),
                ));
            }
        }
        VectorCodec::BFloat16 => {
            for (dst, bytes) in out.iter_mut().zip(row.chunks_exact(2)) {
                *dst = bf16_to_f32(u16::from_le_bytes(bytes.try_into().unwrap()));
            }
        }
        VectorCodec::Int8 => decode_int8(row, out),
        VectorCodec::UInt8 => decode_uint8(row, out),
        VectorCodec::Bit1 => decode_bit1(row, out),
        VectorCodec::TurboQuant2 | VectorCodec::TurboQuant3 | VectorCodec::TurboQuant4 => {
            decode_turboquant(row, dim, codec.bits().unwrap(), out)?
        }
    }
    if out.iter().any(|value| !value.is_finite()) {
        return Err(invalid_data("decoded vector contains non-finite values"));
    }
    Ok(())
}

/// Heap TopK over fixed-width compressed rows. The compressed column is scanned
/// once and only one decoded row plus O(k) heap state is materialized.
pub fn topk_encoded_rows(
    data: &[u8],
    dim: usize,
    codec: VectorCodec,
    computer: &crate::compute::vector_ops::DistanceComputer,
    k: usize,
) -> io::Result<Vec<(usize, f32)>> {
    if k == 0 || data.is_empty() { return Ok(Vec::new()); }
    let stride = codec.row_width(dim)?;
    if stride == 0 || data.len() % stride != 0 {
        return Err(invalid_data("quantized vector column has an invalid byte length"));
    }
    if computer.query.len() != dim {
        return Err(invalid_input(format!(
            "query dimension {} does not match column dimension {dim}",
            computer.query.len()
        )));
    }
    #[derive(Clone, Copy)]
    struct Entry { score: f32, row: usize }
    impl PartialEq for Entry { fn eq(&self, other: &Self) -> bool { self.score == other.score && self.row == other.row } }
    impl Eq for Entry {}
    impl PartialOrd for Entry { fn partial_cmp(&self, other: &Self) -> Option<Ordering> { Some(self.cmp(other)) } }
    impl Ord for Entry {
        fn cmp(&self, other: &Self) -> Ordering {
            self.score.total_cmp(&other.score).then_with(|| self.row.cmp(&other.row))
        }
    }
    let rows = data.len() / stride;
    let mut heap = BinaryHeap::with_capacity(k.min(rows) + 1);
    let mut decoded = vec![0.0f32; dim];
    for (row, bytes) in data.chunks_exact(stride).enumerate() {
        decode_vector(codec, bytes, dim, &mut decoded)?;
        let score = computer.compute_topk_ordering(&decoded);
        let entry = Entry { score, row };
        if heap.len() < k.min(rows) { heap.push(entry); }
        else if heap.peek().is_some_and(|worst| entry < *worst) { heap.pop(); heap.push(entry); }
    }
    let mut result = heap.into_iter().map(|entry| {
        (entry.row, computer.finalize_topk_distance(entry.score))
    }).collect::<Vec<_>>();
    result.sort_unstable_by(|left, right| left.1.total_cmp(&right.1).then_with(|| left.0.cmp(&right.0)));
    Ok(result)
}

pub fn batch_topk_encoded_rows(
    data: &[u8],
    dim: usize,
    codec: VectorCodec,
    queries: &[f32],
    n_queries: usize,
    k: usize,
    metric: crate::compute::vector_ops::DistanceMetric,
) -> io::Result<Vec<Vec<(usize, f32)>>> {
    if n_queries == 0 || queries.len() != n_queries.checked_mul(dim).unwrap_or(usize::MAX) {
        return Err(invalid_input("invalid batch query shape"));
    }
    queries.chunks_exact(dim).map(|query| {
        let computer = crate::compute::vector_ops::DistanceComputer::new(metric, query.to_vec());
        topk_encoded_rows(data, dim, codec, &computer, k)
    }).collect()
}

fn encode_int8(values: &[f32], out: &mut Vec<u8>) {
    let max_abs = values.iter().fold(0.0f32, |acc, value| acc.max(value.abs()));
    let scale = if max_abs == 0.0 { 0.0 } else { max_abs / 127.0 };
    out.extend_from_slice(&scale.to_le_bytes());
    for &value in values {
        let quantized = if scale == 0.0 {
            0
        } else {
            (value / scale).round().clamp(-127.0, 127.0) as i8
        };
        out.push(quantized as u8);
    }
}

fn decode_int8(row: &[u8], out: &mut [f32]) {
    let scale = f32::from_le_bytes(row[..4].try_into().unwrap());
    for (dst, &value) in out.iter_mut().zip(&row[4..]) {
        *dst = (value as i8) as f32 * scale;
    }
}

fn encode_uint8(values: &[f32], out: &mut Vec<u8>) {
    let mut min = f32::INFINITY;
    let mut max = f32::NEG_INFINITY;
    for &value in values {
        min = min.min(value);
        max = max.max(value);
    }
    let scale = if max == min { 0.0 } else { (max - min) / 255.0 };
    out.extend_from_slice(&min.to_le_bytes());
    out.extend_from_slice(&scale.to_le_bytes());
    for &value in values {
        let quantized = if scale == 0.0 {
            0
        } else {
            ((value - min) / scale).round().clamp(0.0, 255.0) as u8
        };
        out.push(quantized);
    }
}

fn decode_uint8(row: &[u8], out: &mut [f32]) {
    let min = f32::from_le_bytes(row[..4].try_into().unwrap());
    let scale = f32::from_le_bytes(row[4..8].try_into().unwrap());
    for (dst, &value) in out.iter_mut().zip(&row[8..]) {
        *dst = min + value as f32 * scale;
    }
}

fn encode_bit1(values: &[f32], out: &mut Vec<u8>) {
    let scale = values.iter().map(|value| value.abs()).sum::<f32>() / values.len() as f32;
    out.extend_from_slice(&scale.to_le_bytes());
    let start = out.len();
    out.resize(start + values.len().div_ceil(8), 0);
    for (index, &value) in values.iter().enumerate() {
        if value >= 0.0 {
            out[start + index / 8] |= 1 << (index % 8);
        }
    }
}

fn decode_bit1(row: &[u8], out: &mut [f32]) {
    let scale = f32::from_le_bytes(row[..4].try_into().unwrap());
    for (index, dst) in out.iter_mut().enumerate() {
        let positive = row[4 + index / 8] & (1 << (index % 8)) != 0;
        *dst = if positive { scale } else { -scale };
    }
}

fn turbo_padded_dim(dim: usize) -> io::Result<usize> {
    if dim == 0 {
        return Err(invalid_input("TurboQuant dimension must be greater than zero"));
    }
    dim.checked_next_power_of_two()
        .ok_or_else(|| invalid_input("TurboQuant dimension is too large"))
}

fn encode_turboquant(values: &[f32], bits: u8, out: &mut Vec<u8>) -> io::Result<()> {
    let padded = turbo_padded_dim(values.len())?;
    let norm = values.iter().map(|value| value * value).sum::<f32>().sqrt();
    out.extend_from_slice(&norm.to_le_bytes());
    let mut rotated = vec![0.0f32; padded];
    if norm != 0.0 {
        for (index, (&value, dst)) in values.iter().zip(rotated.iter_mut()).enumerate() {
            *dst = value / norm * random_sign(index);
        }
        normalized_fwht(&mut rotated);
    }
    let sqrt_dim = (padded as f32).sqrt();
    let codebook = turbo_codebook(bits)?;
    let mut indices = Vec::with_capacity(padded);
    for value in rotated {
        indices.push(nearest_centroid(value * sqrt_dim, codebook) as u16);
    }
    pack_indices(&indices, bits, out);
    Ok(())
}

fn decode_turboquant(row: &[u8], dim: usize, bits: u8, out: &mut [f32]) -> io::Result<()> {
    let norm = f32::from_le_bytes(row[..4].try_into().unwrap());
    let padded = turbo_padded_dim(dim)?;
    let codebook = turbo_codebook(bits)?;
    let indices = unpack_indices(&row[4..], padded, bits)?;
    let scale = if padded == 0 { 0.0 } else { norm / (padded as f32).sqrt() };
    let mut rotated = Vec::with_capacity(padded);
    rotated.extend(indices.into_iter().map(|index| codebook[index as usize] * scale));
    normalized_fwht(&mut rotated);
    for index in 0..dim {
        out[index] = rotated[index] * random_sign(index);
    }
    Ok(())
}

#[inline]
fn random_sign(index: usize) -> f32 {
    let mut value = TURBOQUANT_SEED.wrapping_add((index as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15));
    value = (value ^ (value >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    if (value ^ (value >> 31)) & 1 == 0 { 1.0 } else { -1.0 }
}

fn normalized_fwht(values: &mut [f32]) {
    debug_assert!(values.len().is_power_of_two());
    let mut width = 1;
    while width < values.len() {
        for start in (0..values.len()).step_by(width * 2) {
            for offset in 0..width {
                let left = values[start + offset];
                let right = values[start + offset + width];
                values[start + offset] = left + right;
                values[start + offset + width] = left - right;
            }
        }
        width *= 2;
    }
    let scale = 1.0 / (values.len() as f32).sqrt();
    for value in values {
        *value *= scale;
    }
}

fn turbo_codebook(bits: u8) -> io::Result<&'static [f32]> {
    // Lloyd-Max centroids for a standard normal source.  Symmetry is retained
    // explicitly so codec version 1 is deterministic on every platform.
    const B2: [f32; 4] = [-1.510_418, -0.452_780, 0.452_780, 1.510_418];
    const B3: [f32; 8] = [
        -2.151_946, -1.343_909, -0.756_005, -0.245_094,
         0.245_094,  0.756_005,  1.343_909,  2.151_946,
    ];
    const B4: [f32; 16] = [
        -2.732_589, -2.069_017, -1.618_046, -1.256_231,
        -0.942_341, -0.656_759, -0.388_049, -0.128_396,
         0.128_396,  0.388_049,  0.656_759,  0.942_341,
         1.256_231,  1.618_046,  2.069_017,  2.732_589,
    ];
    match bits {
        2 => Ok(&B2),
        3 => Ok(&B3),
        4 => Ok(&B4),
        _ => Err(invalid_input("TurboQuant supports only 2, 3, or 4 bits")),
    }
}

#[inline]
fn nearest_centroid(value: f32, codebook: &[f32]) -> usize {
    let mut best = 0;
    let mut best_distance = f32::INFINITY;
    for (index, &centroid) in codebook.iter().enumerate() {
        let distance = (value - centroid).abs();
        if distance < best_distance {
            best = index;
            best_distance = distance;
        }
    }
    best
}

#[inline]
fn packed_len(count: usize, bits: u8) -> usize {
    count.saturating_mul(bits as usize).div_ceil(8)
}

fn pack_indices(indices: &[u16], bits: u8, out: &mut Vec<u8>) {
    let start = out.len();
    out.resize(start + packed_len(indices.len(), bits), 0);
    let mut bit_offset = 0usize;
    for &index in indices {
        for bit in 0..bits as usize {
            if index & (1 << bit) != 0 {
                let absolute = bit_offset + bit;
                out[start + absolute / 8] |= 1 << (absolute % 8);
            }
        }
        bit_offset += bits as usize;
    }
}

fn unpack_indices(bytes: &[u8], count: usize, bits: u8) -> io::Result<Vec<u16>> {
    let expected = packed_len(count, bits);
    if bytes.len() != expected {
        return Err(invalid_data(format!(
            "packed vector has {} bytes, expected {expected}",
            bytes.len()
        )));
    }
    let mut out = Vec::with_capacity(count);
    let mut bit_offset = 0usize;
    for _ in 0..count {
        let mut value = 0u16;
        for bit in 0..bits as usize {
            let absolute = bit_offset + bit;
            if bytes[absolute / 8] & (1 << (absolute % 8)) != 0 {
                value |= 1 << bit;
            }
        }
        out.push(value);
        bit_offset += bits as usize;
    }
    Ok(out)
}

fn invalid_input(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message.into())
}

fn invalid_data(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.into())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn round_trip(codec: VectorCodec, values: &[f32]) -> Vec<f32> {
        let mut encoded = Vec::new();
        encode_vector(codec, values, &mut encoded).unwrap();
        assert_eq!(encoded.len(), codec.row_width(values.len()).unwrap());
        let mut decoded = vec![0.0; values.len()];
        decode_vector(codec, &encoded, values.len(), &mut decoded).unwrap();
        decoded
    }

    #[test]
    fn fixed_width_codecs_round_trip() {
        let values = [-1.0, -0.25, 0.0, 0.125, 0.75, 2.0, 3.5];
        for (codec, tolerance) in [
            (VectorCodec::Float32, 0.0),
            (VectorCodec::Float16, 0.002),
            (VectorCodec::BFloat16, 0.02),
            (VectorCodec::Int8, 0.03),
            (VectorCodec::UInt8, 0.03),
        ] {
            let decoded = round_trip(codec, &values);
            for (&actual, &expected) in decoded.iter().zip(&values) {
                assert!((actual - expected).abs() <= tolerance, "{codec:?}: {actual} != {expected}");
            }
        }
    }

    #[test]
    fn bit1_is_symmetric_and_packed() {
        let values = [-2.0, -1.0, 0.0, 1.0, 2.0, -3.0, 4.0, -5.0, 6.0];
        let decoded = round_trip(VectorCodec::Bit1, &values);
        let scale = values.iter().map(|value| value.abs()).sum::<f32>() / values.len() as f32;
        assert_eq!(VectorCodec::Bit1.row_width(values.len()).unwrap(), 6);
        for (&actual, &expected) in decoded.iter().zip(&values) {
            assert_eq!(actual, if expected >= 0.0 { scale } else { -scale });
        }
    }

    #[test]
    fn turboquant_is_deterministic_across_byte_boundaries() {
        let values = (0..13).map(|i| (i as f32 * 0.37).sin()).collect::<Vec<_>>();
        for codec in [VectorCodec::TurboQuant2, VectorCodec::TurboQuant3, VectorCodec::TurboQuant4] {
            let mut first = Vec::new();
            let mut second = Vec::new();
            encode_vector(codec, &values, &mut first).unwrap();
            encode_vector(codec, &values, &mut second).unwrap();
            assert_eq!(first, second);
            let decoded = round_trip(codec, &values);
            assert!(decoded.iter().all(|value| value.is_finite()));
        }
    }

    #[test]
    fn zero_and_constant_vectors_are_well_defined() {
        for codec in [VectorCodec::Int8, VectorCodec::UInt8, VectorCodec::Bit1,
            VectorCodec::TurboQuant2, VectorCodec::TurboQuant3, VectorCodec::TurboQuant4] {
            let zero = round_trip(codec, &[0.0; 9]);
            assert!(zero.iter().all(|&value| value == 0.0), "{codec:?}: {zero:?}");
            let constant = round_trip(codec, &[3.25; 9]);
            assert!(constant.iter().all(|value| value.is_finite()));
        }
    }

    #[test]
    fn malformed_and_non_finite_inputs_are_rejected() {
        let mut bytes = Vec::new();
        assert!(encode_vector(VectorCodec::Int8, &[], &mut bytes).is_err());
        assert!(encode_vector(VectorCodec::Int8, &[f32::NAN], &mut bytes).is_err());
        assert!(decode_vector(VectorCodec::TurboQuant3, &[0; 3], 5, &mut [0.0; 5]).is_err());
    }

    #[test]
    fn bfloat16_uses_round_to_nearest_even() {
        assert_eq!(bf16_to_f32(f32_to_bf16(1.0)), 1.0);
        let value = 1.003_906_25f32;
        assert_eq!(bf16_to_f32(f32_to_bf16(value)), 1.0);
    }
}
