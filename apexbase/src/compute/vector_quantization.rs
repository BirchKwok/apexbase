//! Vector storage codecs shared by ingestion, mmap scans, and Arrow projection.
//!
//! Every codec has a fixed row width for a given dimension.  That property is
//! important for V4 row groups: a row can be addressed without an offset table
//! and TopK scans can score the compressed bytes directly.

use std::io;
use std::{cmp::Ordering, collections::BinaryHeap};

use rayon::prelude::*;

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
                out.extend_from_slice(&crate::storage::on_demand::f32_to_f16(value).to_le_bytes());
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

pub fn decode_vector(
    codec: VectorCodec,
    row: &[u8],
    dim: usize,
    out: &mut [f32],
) -> io::Result<()> {
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

#[derive(Clone, Copy)]
struct TopKEntry {
    score: f32,
    row: usize,
}

impl PartialEq for TopKEntry {
    fn eq(&self, other: &Self) -> bool {
        self.score == other.score && self.row == other.row
    }
}

impl Eq for TopKEntry {}

impl PartialOrd for TopKEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for TopKEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        self.score
            .total_cmp(&other.score)
            .then_with(|| self.row.cmp(&other.row))
    }
}

#[cfg(target_arch = "aarch64")]
#[inline]
fn int8_dot_norm(values: &[u8], query: &[f32]) -> (f32, f32) {
    unsafe {
        use std::arch::aarch64::*;
        let mut dot = vdupq_n_f32(0.0);
        let mut norm = vdupq_n_f32(0.0);
        let chunks = values.len() / 16;
        for chunk in 0..chunks {
            let offset = chunk * 16;
            let packed = vld1q_s8(values.as_ptr().add(offset) as *const i8);
            let low = vmovl_s8(vget_low_s8(packed));
            let high = vmovl_high_s8(packed);
            let groups = [
                vcvtq_f32_s32(vmovl_s16(vget_low_s16(low))),
                vcvtq_f32_s32(vmovl_high_s16(low)),
                vcvtq_f32_s32(vmovl_s16(vget_low_s16(high))),
                vcvtq_f32_s32(vmovl_high_s16(high)),
            ];
            for (lane, value) in groups.into_iter().enumerate() {
                let q = vld1q_f32(query.as_ptr().add(offset + lane * 4));
                dot = vfmaq_f32(dot, value, q);
                norm = vfmaq_f32(norm, value, value);
            }
        }
        let mut dot_sum = vaddvq_f32(dot);
        let mut norm_sum = vaddvq_f32(norm);
        for index in chunks * 16..values.len() {
            let value = (values[index] as i8) as f32;
            dot_sum += value * query[index];
            norm_sum += value * value;
        }
        (dot_sum, norm_sum)
    }
}

#[cfg(not(any(target_arch = "aarch64", target_arch = "x86_64")))]
#[inline]
fn int8_dot_norm(values: &[u8], query: &[f32]) -> (f32, f32) {
    values
        .iter()
        .zip(query)
        .fold((0.0, 0.0), |(dot, norm), (&value, &q)| {
            let value = (value as i8) as f32;
            (dot + value * q, norm + value * value)
        })
}

#[cfg(target_arch = "x86_64")]
#[inline]
fn int8_dot_norm(values: &[u8], query: &[f32]) -> (f32, f32) {
    if std::arch::is_x86_feature_detected!("avx2") && std::arch::is_x86_feature_detected!("fma") {
        unsafe { int8_dot_norm_avx2(values, query) }
    } else {
        values
            .iter()
            .zip(query)
            .fold((0.0, 0.0), |(dot, norm), (&value, &q)| {
                let value = (value as i8) as f32;
                (dot + value * q, norm + value * value)
            })
    }
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2,fma")]
unsafe fn int8_dot_norm_avx2(values: &[u8], query: &[f32]) -> (f32, f32) {
    use std::arch::x86_64::*;
    let mut dot = _mm256_setzero_ps();
    let mut norm = _mm256_setzero_ps();
    let chunks = values.len() / 8;
    for chunk in 0..chunks {
        let offset = chunk * 8;
        let packed = _mm_loadl_epi64(values.as_ptr().add(offset) as *const __m128i);
        let value = _mm256_cvtepi32_ps(_mm256_cvtepi8_epi32(packed));
        let q = _mm256_loadu_ps(query.as_ptr().add(offset));
        dot = _mm256_fmadd_ps(value, q, dot);
        norm = _mm256_fmadd_ps(value, value, norm);
    }
    let mut dot_lanes = [0.0; 8];
    let mut norm_lanes = [0.0; 8];
    _mm256_storeu_ps(dot_lanes.as_mut_ptr(), dot);
    _mm256_storeu_ps(norm_lanes.as_mut_ptr(), norm);
    let mut dot_sum = dot_lanes.into_iter().sum::<f32>();
    let mut norm_sum = norm_lanes.into_iter().sum::<f32>();
    for index in chunks * 8..values.len() {
        let value = (values[index] as i8) as f32;
        dot_sum += value * query[index];
        norm_sum += value * value;
    }
    (dot_sum, norm_sum)
}

#[cfg(target_arch = "aarch64")]
#[inline]
fn int8_l2_squared(values: &[u8], query: &[f32], scale: f32) -> f32 {
    unsafe {
        use std::arch::aarch64::*;
        let mut sums = [vdupq_n_f32(0.0); 4];
        let chunks = values.len() / 16;
        for chunk in 0..chunks {
            let offset = chunk * 16;
            let packed = vld1q_s8(values.as_ptr().add(offset) as *const i8);
            let low = vmovl_s8(vget_low_s8(packed));
            let high = vmovl_high_s8(packed);
            let groups = [
                vcvtq_f32_s32(vmovl_s16(vget_low_s16(low))),
                vcvtq_f32_s32(vmovl_high_s16(low)),
                vcvtq_f32_s32(vmovl_s16(vget_low_s16(high))),
                vcvtq_f32_s32(vmovl_high_s16(high)),
            ];
            for (lane, value) in groups.into_iter().enumerate() {
                let q = vld1q_f32(query.as_ptr().add(offset + lane * 4));
                let diff = vfmsq_n_f32(q, value, scale);
                sums[lane] = vfmaq_f32(sums[lane], diff, diff);
            }
        }
        let sum = vaddq_f32(vaddq_f32(sums[0], sums[1]), vaddq_f32(sums[2], sums[3]));
        let mut result = vaddvq_f32(sum);
        for index in chunks * 16..values.len() {
            let diff = (values[index] as i8) as f32 * scale - query[index];
            result += diff * diff;
        }
        result
    }
}

#[cfg(not(any(target_arch = "aarch64", target_arch = "x86_64")))]
#[inline]
fn int8_l2_squared(values: &[u8], query: &[f32], scale: f32) -> f32 {
    values
        .iter()
        .zip(query)
        .map(|(&value, &q)| {
            let diff = (value as i8) as f32 * scale - q;
            diff * diff
        })
        .sum()
}

#[cfg(target_arch = "x86_64")]
#[inline]
fn int8_l2_squared(values: &[u8], query: &[f32], scale: f32) -> f32 {
    if std::arch::is_x86_feature_detected!("avx2") && std::arch::is_x86_feature_detected!("fma") {
        unsafe { int8_l2_squared_avx2(values, query, scale) }
    } else {
        values
            .iter()
            .zip(query)
            .map(|(&value, &q)| {
                let diff = (value as i8) as f32 * scale - q;
                diff * diff
            })
            .sum()
    }
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2,fma")]
unsafe fn int8_l2_squared_avx2(values: &[u8], query: &[f32], scale: f32) -> f32 {
    use std::arch::x86_64::*;
    let mut sums = [_mm256_setzero_ps(); 4];
    let scale_vec = _mm256_set1_ps(scale);
    let chunks = values.len() / 8;
    for chunk in 0..chunks {
        let offset = chunk * 8;
        let packed = _mm_loadl_epi64(values.as_ptr().add(offset) as *const __m128i);
        let value = _mm256_cvtepi32_ps(_mm256_cvtepi8_epi32(packed));
        let decoded = _mm256_mul_ps(value, scale_vec);
        let diff = _mm256_sub_ps(decoded, _mm256_loadu_ps(query.as_ptr().add(offset)));
        let lane = chunk & 3;
        sums[lane] = _mm256_fmadd_ps(diff, diff, sums[lane]);
    }
    let sum = _mm256_add_ps(
        _mm256_add_ps(sums[0], sums[1]),
        _mm256_add_ps(sums[2], sums[3]),
    );
    let mut lanes = [0.0; 8];
    _mm256_storeu_ps(lanes.as_mut_ptr(), sum);
    let mut result = lanes.into_iter().sum::<f32>();
    for index in chunks * 8..values.len() {
        let diff = (values[index] as i8) as f32 * scale - query[index];
        result += diff * diff;
    }
    result
}

#[cfg(target_arch = "aarch64")]
#[inline]
fn uint8_dot_sum_norm(values: &[u8], query: &[f32]) -> (f32, f32, f32) {
    unsafe {
        use std::arch::aarch64::*;
        let mut dot = vdupq_n_f32(0.0);
        let mut sum = vdupq_n_f32(0.0);
        let mut norm = vdupq_n_f32(0.0);
        let chunks = values.len() / 16;
        for chunk in 0..chunks {
            let offset = chunk * 16;
            let packed = vld1q_u8(values.as_ptr().add(offset));
            let low = vmovl_u8(vget_low_u8(packed));
            let high = vmovl_high_u8(packed);
            let groups = [
                vcvtq_f32_u32(vmovl_u16(vget_low_u16(low))),
                vcvtq_f32_u32(vmovl_high_u16(low)),
                vcvtq_f32_u32(vmovl_u16(vget_low_u16(high))),
                vcvtq_f32_u32(vmovl_high_u16(high)),
            ];
            for (lane, value) in groups.into_iter().enumerate() {
                let q = vld1q_f32(query.as_ptr().add(offset + lane * 4));
                dot = vfmaq_f32(dot, value, q);
                sum = vaddq_f32(sum, value);
                norm = vfmaq_f32(norm, value, value);
            }
        }
        let mut dot_sum = vaddvq_f32(dot);
        let mut value_sum = vaddvq_f32(sum);
        let mut norm_sum = vaddvq_f32(norm);
        for index in chunks * 16..values.len() {
            let value = values[index] as f32;
            dot_sum += value * query[index];
            value_sum += value;
            norm_sum += value * value;
        }
        (dot_sum, value_sum, norm_sum)
    }
}

#[cfg(target_arch = "aarch64")]
#[inline]
fn uint8_l2_squared(values: &[u8], query: &[f32], min: f32, scale: f32) -> f32 {
    unsafe {
        use std::arch::aarch64::*;
        let mut sums = [vdupq_n_f32(0.0); 4];
        let min_vec = vdupq_n_f32(min);
        let chunks = values.len() / 16;
        for chunk in 0..chunks {
            let offset = chunk * 16;
            let packed = vld1q_u8(values.as_ptr().add(offset));
            let low = vmovl_u8(vget_low_u8(packed));
            let high = vmovl_high_u8(packed);
            let groups = [
                vcvtq_f32_u32(vmovl_u16(vget_low_u16(low))),
                vcvtq_f32_u32(vmovl_high_u16(low)),
                vcvtq_f32_u32(vmovl_u16(vget_low_u16(high))),
                vcvtq_f32_u32(vmovl_high_u16(high)),
            ];
            for (lane, value) in groups.into_iter().enumerate() {
                let decoded = vfmaq_n_f32(min_vec, value, scale);
                let diff = vsubq_f32(decoded, vld1q_f32(query.as_ptr().add(offset + lane * 4)));
                sums[lane] = vfmaq_f32(sums[lane], diff, diff);
            }
        }
        let sum = vaddq_f32(vaddq_f32(sums[0], sums[1]), vaddq_f32(sums[2], sums[3]));
        let mut result = vaddvq_f32(sum);
        for index in chunks * 16..values.len() {
            let diff = min + values[index] as f32 * scale - query[index];
            result += diff * diff;
        }
        result
    }
}

#[cfg(not(any(target_arch = "aarch64", target_arch = "x86_64")))]
#[inline]
fn uint8_l2_squared(values: &[u8], query: &[f32], min: f32, scale: f32) -> f32 {
    values
        .iter()
        .zip(query)
        .map(|(&value, &q)| {
            let diff = min + value as f32 * scale - q;
            diff * diff
        })
        .sum()
}

#[cfg(target_arch = "x86_64")]
#[inline]
fn uint8_l2_squared(values: &[u8], query: &[f32], min: f32, scale: f32) -> f32 {
    if std::arch::is_x86_feature_detected!("avx2") && std::arch::is_x86_feature_detected!("fma") {
        unsafe { uint8_l2_squared_avx2(values, query, min, scale) }
    } else {
        values
            .iter()
            .zip(query)
            .map(|(&value, &q)| {
                let diff = min + value as f32 * scale - q;
                diff * diff
            })
            .sum()
    }
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2,fma")]
unsafe fn uint8_l2_squared_avx2(values: &[u8], query: &[f32], min: f32, scale: f32) -> f32 {
    use std::arch::x86_64::*;
    let mut sums = [_mm256_setzero_ps(); 4];
    let min_vec = _mm256_set1_ps(min);
    let scale_vec = _mm256_set1_ps(scale);
    let chunks = values.len() / 8;
    for chunk in 0..chunks {
        let offset = chunk * 8;
        let packed = _mm_loadl_epi64(values.as_ptr().add(offset) as *const __m128i);
        let value = _mm256_cvtepi32_ps(_mm256_cvtepu8_epi32(packed));
        let decoded = _mm256_fmadd_ps(value, scale_vec, min_vec);
        let diff = _mm256_sub_ps(decoded, _mm256_loadu_ps(query.as_ptr().add(offset)));
        let lane = chunk & 3;
        sums[lane] = _mm256_fmadd_ps(diff, diff, sums[lane]);
    }
    let sum = _mm256_add_ps(
        _mm256_add_ps(sums[0], sums[1]),
        _mm256_add_ps(sums[2], sums[3]),
    );
    let mut lanes = [0.0; 8];
    _mm256_storeu_ps(lanes.as_mut_ptr(), sum);
    let mut result = lanes.into_iter().sum::<f32>();
    for index in chunks * 8..values.len() {
        let diff = min + values[index] as f32 * scale - query[index];
        result += diff * diff;
    }
    result
}

#[cfg(not(any(target_arch = "aarch64", target_arch = "x86_64")))]
#[inline]
fn uint8_dot_sum_norm(values: &[u8], query: &[f32]) -> (f32, f32, f32) {
    values
        .iter()
        .zip(query)
        .fold((0.0, 0.0, 0.0), |(dot, sum, norm), (&value, &q)| {
            let value = value as f32;
            (dot + value * q, sum + value, norm + value * value)
        })
}

#[cfg(target_arch = "x86_64")]
#[inline]
fn uint8_dot_sum_norm(values: &[u8], query: &[f32]) -> (f32, f32, f32) {
    if std::arch::is_x86_feature_detected!("avx2") && std::arch::is_x86_feature_detected!("fma") {
        unsafe { uint8_dot_sum_norm_avx2(values, query) }
    } else {
        values
            .iter()
            .zip(query)
            .fold((0.0, 0.0, 0.0), |(dot, sum, norm), (&value, &q)| {
                let value = value as f32;
                (dot + value * q, sum + value, norm + value * value)
            })
    }
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2,fma")]
unsafe fn uint8_dot_sum_norm_avx2(values: &[u8], query: &[f32]) -> (f32, f32, f32) {
    use std::arch::x86_64::*;
    let mut dot = _mm256_setzero_ps();
    let mut sum = _mm256_setzero_ps();
    let mut norm = _mm256_setzero_ps();
    let chunks = values.len() / 8;
    for chunk in 0..chunks {
        let offset = chunk * 8;
        let packed = _mm_loadl_epi64(values.as_ptr().add(offset) as *const __m128i);
        let value = _mm256_cvtepi32_ps(_mm256_cvtepu8_epi32(packed));
        let q = _mm256_loadu_ps(query.as_ptr().add(offset));
        dot = _mm256_fmadd_ps(value, q, dot);
        sum = _mm256_add_ps(sum, value);
        norm = _mm256_fmadd_ps(value, value, norm);
    }
    let mut dot_lanes = [0.0; 8];
    let mut sum_lanes = [0.0; 8];
    let mut norm_lanes = [0.0; 8];
    _mm256_storeu_ps(dot_lanes.as_mut_ptr(), dot);
    _mm256_storeu_ps(sum_lanes.as_mut_ptr(), sum);
    _mm256_storeu_ps(norm_lanes.as_mut_ptr(), norm);
    let mut dot_sum = dot_lanes.into_iter().sum::<f32>();
    let mut value_sum = sum_lanes.into_iter().sum::<f32>();
    let mut norm_sum = norm_lanes.into_iter().sum::<f32>();
    for index in chunks * 8..values.len() {
        let value = values[index] as f32;
        dot_sum += value * query[index];
        value_sum += value;
        norm_sum += value * value;
    }
    (dot_sum, value_sum, norm_sum)
}

struct EncodedQuery<'a> {
    codec: VectorCodec,
    dim: usize,
    computer: &'a crate::compute::vector_ops::DistanceComputer,
    query_sum: f32,
    query_norm_sq: f32,
    query_i8: Vec<i8>,
    query_i8_scale: f32,
    bit1_lut: Vec<f32>,
    bit1_query: Vec<u8>,
    turbo_query: Vec<f32>,
    turbo_dot_lut: Vec<f32>,
    turbo_norm_lut: Vec<f32>,
    turbo_codes_per_byte: usize,
}

impl<'a> EncodedQuery<'a> {
    fn new(
        codec: VectorCodec,
        dim: usize,
        computer: &'a crate::compute::vector_ops::DistanceComputer,
    ) -> io::Result<Self> {
        let mut bit1_lut = Vec::new();
        let mut bit1_query = Vec::new();
        if codec == VectorCodec::Bit1 {
            bit1_query.resize(dim.div_ceil(8), 0);
            for (index, &value) in computer.query.iter().enumerate() {
                if value >= 0.0 {
                    bit1_query[index / 8] |= 1 << (index % 8);
                }
            }
            bit1_lut.resize(dim.div_ceil(8) * 256, 0.0);
            for block in 0..dim.div_ceil(8) {
                for pattern in 0..256usize {
                    let mut dot = 0.0;
                    for bit in 0..8 {
                        let index = block * 8 + bit;
                        if index == dim {
                            break;
                        }
                        let sign = if pattern & (1 << bit) != 0 { 1.0 } else { -1.0 };
                        dot += sign * computer.query[index];
                    }
                    bit1_lut[block * 256 + pattern] = dot;
                }
            }
        }

        let mut turbo_query = Vec::new();
        if codec.bits().is_some() {
            let padded = turbo_padded_dim(dim)?;
            turbo_query.resize(padded, 0.0);
            for (index, &value) in computer.query.iter().enumerate() {
                turbo_query[index] = value * random_sign(index);
            }
            normalized_fwht(&mut turbo_query);
        }

        let query_max_abs = computer
            .query
            .iter()
            .fold(0.0f32, |maximum, value| maximum.max(value.abs()));
        let query_i8_scale = if query_max_abs == 0.0 {
            0.0
        } else {
            query_max_abs / 127.0
        };
        let query_i8 = computer
            .query
            .iter()
            .map(|value| {
                if query_i8_scale == 0.0 {
                    0
                } else {
                    (value / query_i8_scale).round().clamp(-127.0, 127.0) as i8
                }
            })
            .collect();

        let mut turbo_dot_lut = Vec::new();
        let mut turbo_norm_lut = Vec::new();
        let mut turbo_codes_per_byte = 0;
        if matches!(codec.bits(), Some(2 | 4)) {
            let bits = codec.bits().unwrap();
            let codebook = turbo_codebook(bits)?;
            turbo_codes_per_byte = 8 / bits as usize;
            let blocks = turbo_query.len() / turbo_codes_per_byte;
            turbo_dot_lut.resize(blocks * 256, 0.0);
            turbo_norm_lut.resize(256, 0.0);
            let mask = (1usize << bits) - 1;
            for byte in 0..256usize {
                let mut norm = 0.0;
                for lane in 0..turbo_codes_per_byte {
                    let centroid = codebook[(byte >> (lane * bits as usize)) & mask];
                    norm += centroid * centroid;
                }
                turbo_norm_lut[byte] = norm;
            }
            for block in 0..blocks {
                for byte in 0..256usize {
                    let mut dot = 0.0;
                    for lane in 0..turbo_codes_per_byte {
                        let centroid = codebook[(byte >> (lane * bits as usize)) & mask];
                        dot += centroid * turbo_query[block * turbo_codes_per_byte + lane];
                    }
                    turbo_dot_lut[block * 256 + byte] = dot;
                }
            }
        }

        Ok(Self {
            codec,
            dim,
            computer,
            query_sum: computer.query.iter().sum(),
            query_norm_sq: computer.query.iter().map(|value| value * value).sum(),
            query_i8,
            query_i8_scale,
            bit1_lut,
            bit1_query,
            turbo_query,
            turbo_dot_lut,
            turbo_norm_lut,
            turbo_codes_per_byte,
        })
    }

    #[inline]
    fn bit1_hamming(&self, row: &[u8]) -> f32 {
        let values = &row[4..];
        let mut mismatches = 0u32;
        let word_bytes = values.len() / 8 * 8;
        for offset in (0..word_bytes).step_by(8) {
            let value = u64::from_le_bytes(values[offset..offset + 8].try_into().unwrap());
            let query = u64::from_le_bytes(self.bit1_query[offset..offset + 8].try_into().unwrap());
            mismatches += (value ^ query).count_ones();
        }
        for index in word_bytes..values.len() {
            mismatches += (values[index] ^ self.bit1_query[index]).count_ones();
        }
        mismatches as f32
    }

    #[inline]
    fn bit1_weighted_l2(&self, row: &[u8]) -> f32 {
        let scale = f32::from_le_bytes(row[..4].try_into().unwrap());
        let dot = row[4..]
            .iter()
            .enumerate()
            .map(|(block, &value)| self.bit1_lut[block * 256 + value as usize])
            .sum::<f32>()
            * scale;
        (scale * scale * self.dim as f32 + self.query_norm_sq - 2.0 * dot).max(0.0)
    }

    #[inline]
    fn int8_quantized_query_l2(&self, row: &[u8]) -> f32 {
        let row_scale = f32::from_le_bytes(row[..4].try_into().unwrap());
        let mut dot = 0i32;
        let mut norm = 0i32;
        for (&value, &query) in row[4..].iter().zip(&self.query_i8) {
            let value = (value as i8) as i32;
            dot += value * query as i32;
            norm += value * value;
        }
        (row_scale * row_scale * norm as f32 + self.query_norm_sq
            - 2.0 * row_scale * self.query_i8_scale * dot as f32)
            .max(0.0)
    }

    #[inline]
    fn uint8_quantized_query_l2(&self, row: &[u8]) -> f32 {
        let min = f32::from_le_bytes(row[..4].try_into().unwrap());
        let row_scale = f32::from_le_bytes(row[4..8].try_into().unwrap());
        let offset = min + 128.0 * row_scale;
        let mut dot = 0i32;
        let mut sum = 0i32;
        let mut norm = 0i32;
        for (&value, &query) in row[8..].iter().zip(&self.query_i8) {
            let value = value as i32 - 128;
            dot += value * query as i32;
            sum += value;
            norm += value * value;
        }
        let row_norm = self.dim as f32 * offset * offset
            + 2.0 * offset * row_scale * sum as f32
            + row_scale * row_scale * norm as f32;
        let row_dot = offset * self.query_sum + row_scale * self.query_i8_scale * dot as f32;
        (row_norm + self.query_norm_sq - 2.0 * row_dot).max(0.0)
    }

    #[inline]
    fn ordering_from_dot_norm(&self, dot: f32, row_norm_sq: f32) -> Option<f32> {
        use crate::compute::vector_ops::DistanceMetric;
        match self.computer.metric {
            DistanceMetric::L2 | DistanceMetric::L2Squared => Some(
                self.computer
                    .compute_l2_topk_ordering_from_dot(dot, row_norm_sq),
            ),
            DistanceMetric::InnerProduct | DistanceMetric::NegInnerProduct => {
                Some(self.computer.compute_dot_topk_ordering_from_dot(dot))
            }
            DistanceMetric::CosineSimilarity | DistanceMetric::CosineDistance => {
                let recip = if row_norm_sq > 0.0 {
                    1.0 / row_norm_sq.sqrt()
                } else {
                    0.0
                };
                Some(
                    self.computer
                        .compute_cosine_topk_ordering_from_dot(dot, recip),
                )
            }
            DistanceMetric::L1 | DistanceMetric::LInf => None,
        }
    }

    #[inline]
    fn score(&self, row: &[u8], decoded: &mut [f32]) -> io::Result<f32> {
        let direct = match self.codec {
            VectorCodec::BFloat16 => {
                if matches!(
                    self.computer.metric,
                    crate::compute::vector_ops::DistanceMetric::L2
                        | crate::compute::vector_ops::DistanceMetric::L2Squared
                ) {
                    let mut l2 = 0.0;
                    for (bytes, &query) in row.chunks_exact(2).zip(&self.computer.query) {
                        let value = bf16_to_f32(u16::from_le_bytes(bytes.try_into().unwrap()));
                        let diff = value - query;
                        l2 += diff * diff;
                    }
                    return Ok(l2);
                }
                let mut dot = 0.0;
                let mut norm = 0.0;
                for (bytes, &query) in row.chunks_exact(2).zip(&self.computer.query) {
                    let value = bf16_to_f32(u16::from_le_bytes(bytes.try_into().unwrap()));
                    dot += value * query;
                    norm += value * value;
                }
                self.ordering_from_dot_norm(dot, norm)
            }
            VectorCodec::Int8 => {
                let scale = f32::from_le_bytes(row[..4].try_into().unwrap());
                if matches!(
                    self.computer.metric,
                    crate::compute::vector_ops::DistanceMetric::L2
                        | crate::compute::vector_ops::DistanceMetric::L2Squared
                ) {
                    return Ok(int8_l2_squared(&row[4..], &self.computer.query, scale));
                }
                let (dot, norm) = int8_dot_norm(&row[4..], &self.computer.query);
                self.ordering_from_dot_norm(dot * scale, norm * scale * scale)
            }
            VectorCodec::UInt8 => {
                let min = f32::from_le_bytes(row[..4].try_into().unwrap());
                let scale = f32::from_le_bytes(row[4..8].try_into().unwrap());
                if matches!(
                    self.computer.metric,
                    crate::compute::vector_ops::DistanceMetric::L2
                        | crate::compute::vector_ops::DistanceMetric::L2Squared
                ) {
                    return Ok(uint8_l2_squared(
                        &row[8..],
                        &self.computer.query,
                        min,
                        scale,
                    ));
                }
                let (dot_u8, sum, sum_sq) = uint8_dot_sum_norm(&row[8..], &self.computer.query);
                let dot = min * self.query_sum + scale * dot_u8;
                let norm =
                    self.dim as f32 * min * min + 2.0 * min * scale * sum + scale * scale * sum_sq;
                self.ordering_from_dot_norm(dot, norm)
            }
            VectorCodec::Bit1 => {
                if matches!(
                    self.computer.metric,
                    crate::compute::vector_ops::DistanceMetric::L2
                        | crate::compute::vector_ops::DistanceMetric::L2Squared
                ) {
                    return Ok(self.bit1_hamming(row));
                }
                let scale = f32::from_le_bytes(row[..4].try_into().unwrap());
                let dot = row[4..]
                    .iter()
                    .enumerate()
                    .map(|(block, &value)| self.bit1_lut[block * 256 + value as usize])
                    .sum::<f32>()
                    * scale;
                self.ordering_from_dot_norm(dot, scale * scale * self.dim as f32)
            }
            VectorCodec::TurboQuant2 | VectorCodec::TurboQuant3 | VectorCodec::TurboQuant4 => {
                use crate::compute::vector_ops::DistanceMetric;
                let needs_norm = matches!(
                    self.computer.metric,
                    DistanceMetric::L2
                        | DistanceMetric::L2Squared
                        | DistanceMetric::CosineSimilarity
                        | DistanceMetric::CosineDistance
                );
                if needs_norm && !self.dim.is_power_of_two() {
                    None
                } else {
                    let norm = f32::from_le_bytes(row[..4].try_into().unwrap());
                    let bits = self.codec.bits().unwrap();
                    let codebook = turbo_codebook(bits)?;
                    let padded = self.turbo_query.len();
                    let scale = norm / (padded as f32).sqrt();
                    let mut dot = 0.0;
                    let mut code_norm = 0.0;
                    if self.turbo_codes_per_byte != 0 {
                        for (block, &byte) in row[4..].iter().enumerate() {
                            dot += self.turbo_dot_lut[block * 256 + byte as usize];
                            code_norm += self.turbo_norm_lut[byte as usize];
                        }
                    } else {
                        let mask = (1u64 << bits) - 1;
                        let mut reservoir = 0u64;
                        let mut available = 0u8;
                        let mut input = row[4..].iter();
                        for &query in &self.turbo_query {
                            while available < bits {
                                reservoir |= (*input.next().unwrap() as u64) << available;
                                available += 8;
                            }
                            let centroid = codebook[(reservoir & mask) as usize];
                            reservoir >>= bits;
                            available -= bits;
                            dot += centroid * query;
                            code_norm += centroid * centroid;
                        }
                    }
                    self.ordering_from_dot_norm(dot * scale, code_norm * scale * scale)
                }
            }
            VectorCodec::Float32 | VectorCodec::Float16 => None,
        };

        if let Some(score) = direct {
            return Ok(score);
        }
        decode_vector(self.codec, row, self.dim, decoded)?;
        Ok(self.computer.compute_topk_ordering(decoded))
    }
}

#[inline]
fn push_topk(heap: &mut BinaryHeap<TopKEntry>, k: usize, entry: TopKEntry) {
    if heap.len() < k {
        heap.push(entry);
    } else if heap.peek().is_some_and(|worst| entry < *worst) {
        heap.pop();
        heap.push(entry);
    }
}

fn scan_encoded_chunk(
    data: &[u8],
    row_base: usize,
    stride: usize,
    scorer: &EncodedQuery<'_>,
    k: usize,
) -> io::Result<BinaryHeap<TopKEntry>> {
    use crate::compute::vector_ops::DistanceMetric;
    if matches!(
        scorer.computer.metric,
        DistanceMetric::L2 | DistanceMetric::L2Squared
    ) {
        match scorer.codec {
            VectorCodec::BFloat16 => {
                return Ok(scan_encoded_chunk_direct(
                    data,
                    row_base,
                    stride,
                    k,
                    |row| {
                        let mut l2 = 0.0;
                        for (bytes, &query) in row.chunks_exact(2).zip(&scorer.computer.query) {
                            let value = bf16_to_f32(u16::from_le_bytes(bytes.try_into().unwrap()));
                            let diff = value - query;
                            l2 += diff * diff;
                        }
                        l2
                    },
                ));
            }
            VectorCodec::Int8 => {
                return Ok(scan_encoded_chunk_direct(
                    data,
                    row_base,
                    stride,
                    k,
                    |row| {
                        if scorer.dim >= 64 {
                            scorer.int8_quantized_query_l2(row)
                        } else {
                            int8_l2_squared(
                                &row[4..],
                                &scorer.computer.query,
                                f32::from_le_bytes(row[..4].try_into().unwrap()),
                            )
                        }
                    },
                ));
            }
            VectorCodec::UInt8 => {
                return Ok(scan_encoded_chunk_direct(
                    data,
                    row_base,
                    stride,
                    k,
                    |row| {
                        if scorer.dim >= 64 {
                            scorer.uint8_quantized_query_l2(row)
                        } else {
                            uint8_l2_squared(
                                &row[8..],
                                &scorer.computer.query,
                                f32::from_le_bytes(row[..4].try_into().unwrap()),
                                f32::from_le_bytes(row[4..8].try_into().unwrap()),
                            )
                        }
                    },
                ));
            }
            VectorCodec::Bit1 => {
                return Ok(scan_bit1_hamming_chunk(data, row_base, stride, scorer, k));
            }
            _ => {}
        }
    }
    let mut heap = BinaryHeap::with_capacity(k + 1);
    let mut decoded = vec![0.0f32; scorer.dim];
    for (offset, bytes) in data.chunks_exact(stride).enumerate() {
        push_topk(
            &mut heap,
            k,
            TopKEntry {
                score: scorer.score(bytes, &mut decoded)?,
                row: row_base + offset,
            },
        );
    }
    Ok(heap)
}

#[inline]
fn scan_bit1_hamming_chunk(
    data: &[u8],
    row_base: usize,
    stride: usize,
    scorer: &EncodedQuery<'_>,
    k: usize,
) -> BinaryHeap<TopKEntry> {
    let scores = scorer.dim + 1;
    let candidate_k = if scorer.dim >= 64 {
        k.saturating_mul(8).max(64)
    } else {
        k
    };
    let mut counts = vec![0usize; scores];
    let mut buckets = vec![0usize; scores.saturating_mul(candidate_k)];
    for (offset, row) in data.chunks_exact(stride).enumerate() {
        let score = scorer.bit1_hamming(row) as usize;
        let count = counts[score];
        if count < candidate_k {
            buckets[score * candidate_k + count] = row_base + offset;
            counts[score] = count + 1;
        }
    }
    let mut candidates = Vec::with_capacity(candidate_k);
    for (score, &count) in counts.iter().enumerate() {
        for index in 0..count {
            candidates.push(TopKEntry {
                score: score as f32,
                row: buckets[score * candidate_k + index],
            });
            if candidates.len() == candidate_k {
                break;
            }
        }
        if candidates.len() == candidate_k {
            break;
        }
    }
    if candidate_k == k {
        return BinaryHeap::from(candidates);
    }
    let mut reranked = DirectTopK::new(k);
    for candidate in candidates {
        let local_row = candidate.row - row_base;
        let start = local_row * stride;
        reranked.push(TopKEntry {
            score: scorer.bit1_weighted_l2(&data[start..start + stride]),
            row: candidate.row,
        });
    }
    BinaryHeap::from(reranked.entries)
}

#[inline]
fn scan_encoded_chunk_direct(
    data: &[u8],
    row_base: usize,
    stride: usize,
    k: usize,
    mut score: impl FnMut(&[u8]) -> f32,
) -> BinaryHeap<TopKEntry> {
    let mut topk = DirectTopK::new(k);
    for (offset, row) in data.chunks_exact(stride).enumerate() {
        topk.push(TopKEntry {
            score: score(row),
            row: row_base + offset,
        });
    }
    BinaryHeap::from(topk.entries)
}

struct DirectTopK {
    entries: Vec<TopKEntry>,
    capacity: usize,
    worst: usize,
}

impl DirectTopK {
    #[inline]
    fn new(capacity: usize) -> Self {
        Self {
            entries: Vec::with_capacity(capacity),
            capacity,
            worst: 0,
        }
    }

    #[inline]
    fn push(&mut self, entry: TopKEntry) {
        if self.entries.len() < self.capacity {
            self.entries.push(entry);
            if self.entries.len() == self.capacity {
                self.refresh_worst();
            }
        } else if entry < self.entries[self.worst] {
            self.entries[self.worst] = entry;
            self.refresh_worst();
        }
    }

    #[inline]
    fn refresh_worst(&mut self) {
        self.worst = self
            .entries
            .iter()
            .enumerate()
            .max_by(|left, right| left.1.cmp(right.1))
            .map_or(0, |(index, _)| index);
    }
}

/// Parallel heap TopK over fixed-width compressed rows. Dot-product-compatible
/// metrics are scored directly from encoded bytes; only L1/Linf decode rows.
pub fn topk_encoded_rows(
    data: &[u8],
    dim: usize,
    codec: VectorCodec,
    computer: &crate::compute::vector_ops::DistanceComputer,
    k: usize,
) -> io::Result<Vec<(usize, f32)>> {
    topk_encoded_rows_impl(data, dim, codec, computer, k, true)
}

fn topk_encoded_rows_impl(
    data: &[u8],
    dim: usize,
    codec: VectorCodec,
    computer: &crate::compute::vector_ops::DistanceComputer,
    k: usize,
    allow_parallel: bool,
) -> io::Result<Vec<(usize, f32)>> {
    if k == 0 || data.is_empty() {
        return Ok(Vec::new());
    }
    let stride = codec.row_width(dim)?;
    if stride == 0 || data.len() % stride != 0 {
        return Err(invalid_data(
            "quantized vector column has an invalid byte length",
        ));
    }
    if computer.query.len() != dim {
        return Err(invalid_input(format!(
            "query dimension {} does not match column dimension {dim}",
            computer.query.len()
        )));
    }
    let rows = data.len() / stride;
    let k = k.min(rows);
    let scorer = EncodedQuery::new(codec, dim, computer)?;
    let chunk_rows = rows.div_ceil(rayon::current_num_threads()).max(2_048);
    let chunk_bytes = chunk_rows.saturating_mul(stride);
    let local_heaps = if allow_parallel && rows >= 4_096 && rayon::current_num_threads() > 1 {
        data.par_chunks(chunk_bytes)
            .enumerate()
            .map(|(chunk, bytes)| scan_encoded_chunk(bytes, chunk * chunk_rows, stride, &scorer, k))
            .collect::<Vec<_>>()
    } else {
        vec![scan_encoded_chunk(data, 0, stride, &scorer, k)]
    };
    let mut heap = BinaryHeap::with_capacity(k + 1);
    for local in local_heaps {
        for entry in local? {
            push_topk(&mut heap, k, entry);
        }
    }
    let mut result = heap
        .into_iter()
        .map(|entry| (entry.row, computer.finalize_topk_distance(entry.score)))
        .collect::<Vec<_>>();
    result.sort_unstable_by(|left, right| {
        left.1
            .total_cmp(&right.1)
            .then_with(|| left.0.cmp(&right.0))
    });
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
    queries
        .par_chunks_exact(dim)
        .map(|query| {
            let computer =
                crate::compute::vector_ops::DistanceComputer::new(metric, query.to_vec());
            topk_encoded_rows_impl(data, dim, codec, &computer, k, false)
        })
        .collect()
}

fn encode_int8(values: &[f32], out: &mut Vec<u8>) {
    let max_abs = values
        .iter()
        .fold(0.0f32, |acc, value| acc.max(value.abs()));
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
        return Err(invalid_input(
            "TurboQuant dimension must be greater than zero",
        ));
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
    let scale = if padded == 0 {
        0.0
    } else {
        norm / (padded as f32).sqrt()
    };
    let mut rotated = Vec::with_capacity(padded);
    rotated.extend(
        indices
            .into_iter()
            .map(|index| codebook[index as usize] * scale),
    );
    normalized_fwht(&mut rotated);
    for index in 0..dim {
        out[index] = rotated[index] * random_sign(index);
    }
    Ok(())
}

#[inline]
fn random_sign(index: usize) -> f32 {
    let mut value =
        TURBOQUANT_SEED.wrapping_add((index as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15));
    value = (value ^ (value >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    if (value ^ (value >> 31)) & 1 == 0 {
        1.0
    } else {
        -1.0
    }
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
        -2.151_946, -1.343_909, -0.756_005, -0.245_094, 0.245_094, 0.756_005, 1.343_909, 2.151_946,
    ];
    const B4: [f32; 16] = [
        -2.732_589, -2.069_017, -1.618_046, -1.256_231, -0.942_341, -0.656_759, -0.388_049,
        -0.128_396, 0.128_396, 0.388_049, 0.656_759, 0.942_341, 1.256_231, 1.618_046, 2.069_017,
        2.732_589,
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
                assert!(
                    (actual - expected).abs() <= tolerance,
                    "{codec:?}: {actual} != {expected}"
                );
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
        for codec in [
            VectorCodec::TurboQuant2,
            VectorCodec::TurboQuant3,
            VectorCodec::TurboQuant4,
        ] {
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
        for codec in [
            VectorCodec::Int8,
            VectorCodec::UInt8,
            VectorCodec::Bit1,
            VectorCodec::TurboQuant2,
            VectorCodec::TurboQuant3,
            VectorCodec::TurboQuant4,
        ] {
            let zero = round_trip(codec, &[0.0; 9]);
            assert!(
                zero.iter().all(|&value| value == 0.0),
                "{codec:?}: {zero:?}"
            );
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

    #[test]
    fn encoded_topk_matches_decoded_distance_ordering() {
        use crate::compute::vector_ops::{DistanceComputer, DistanceMetric};

        for dim in [13, 16] {
            let rows = (0..257)
                .map(|row| {
                    (0..dim)
                        .map(|col| ((row * 17 + col * 29) as f32 * 0.031).sin())
                        .collect::<Vec<_>>()
                })
                .collect::<Vec<_>>();
            let query = (0..dim)
                .map(|col| ((col * 11 + 7) as f32 * 0.071).cos())
                .collect::<Vec<_>>();

            for codec in [
                VectorCodec::BFloat16,
                VectorCodec::Int8,
                VectorCodec::UInt8,
                VectorCodec::Bit1,
                VectorCodec::TurboQuant2,
                VectorCodec::TurboQuant3,
                VectorCodec::TurboQuant4,
            ] {
                let mut encoded = Vec::new();
                for row in &rows {
                    encode_vector(codec, row, &mut encoded).unwrap();
                }
                for metric in [
                    DistanceMetric::L2,
                    DistanceMetric::L2Squared,
                    DistanceMetric::InnerProduct,
                    DistanceMetric::CosineDistance,
                    DistanceMetric::L1,
                    DistanceMetric::LInf,
                ] {
                    let computer = DistanceComputer::new(metric, query.clone());
                    let actual = topk_encoded_rows(&encoded, dim, codec, &computer, 12).unwrap();
                    let mut decoded = vec![0.0; dim];
                    let stride = codec.row_width(dim).unwrap();
                    let mut expected = encoded
                        .chunks_exact(stride)
                        .enumerate()
                        .map(|(index, bytes)| {
                            decode_vector(codec, bytes, dim, &mut decoded).unwrap();
                            let score = if codec == VectorCodec::Bit1
                                && matches!(metric, DistanceMetric::L2 | DistanceMetric::L2Squared)
                            {
                                decoded
                                    .iter()
                                    .zip(&query)
                                    .filter(|(value, query)| (**value >= 0.0) != (**query >= 0.0))
                                    .count() as f32
                            } else {
                                computer.compute_topk_ordering(&decoded)
                            };
                            (index, score)
                        })
                        .collect::<Vec<_>>();
                    expected.sort_unstable_by(|left, right| {
                        left.1
                            .total_cmp(&right.1)
                            .then_with(|| left.0.cmp(&right.0))
                    });
                    let cutoff = expected[11].1;
                    let expected_scores = expected
                        .iter()
                        .copied()
                        .collect::<std::collections::HashMap<_, _>>();
                    expected.truncate(12);
                    assert_eq!(actual.len(), expected.len());
                    for (actual_row, actual_distance) in &actual {
                        let expected_score = expected_scores[actual_row];
                        assert!(
                            expected_score <= cutoff + 2e-4,
                            "{codec:?} {metric:?} dim={dim}: row {actual_row} missed cutoff"
                        );
                        let expected_distance = computer.finalize_topk_distance(expected_score);
                        assert!(
                            (actual_distance - expected_distance).abs() <= 2e-4,
                            "{codec:?} {metric:?} dim={dim}: {actual_distance} != {expected_distance}"
                        );
                    }
                }
            }
        }
    }
}
