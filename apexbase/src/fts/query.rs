fn phrase_query(query: &str) -> (&str, bool) {
    let query = query.trim();
    if query.len() >= 2 && query.starts_with('"') && query.ends_with('"') {
        (&query[1..query.len() - 1], true)
    } else {
        (query, false)
    }
}

fn matching_docs(state: &IndexState, analyzed: &AnalyzedDocument, phrase: bool) -> FtsResult<RoaringTreemap> {
    let mut postings: Vec<RoaringTreemap> = analyzed
        .terms
        .iter()
        .map(|term| state.posting(term))
        .collect::<FtsResult<Vec<_>>>()?;
    postings.sort_unstable_by_key(RoaringTreemap::len);
    let mut postings = postings.into_iter();
    let Some(mut result) = postings.next() else {
        return Ok(RoaringTreemap::new());
    };
    for posting in postings {
        result &= posting;
        if result.is_empty() {
            return Ok(result);
        }
    }
    if !phrase {
        return Ok(result);
    }

    let query_tokens: Vec<u32> = analyzed
        .tokens
        .iter()
        .filter_map(|&local_id| {
            if local_id == 0 {
                None
            } else {
                analyzed
                    .terms
                    .get(local_id as usize - 1)
                    .and_then(|term| state.term_id(term))
            }
        })
        .collect();
    if query_tokens.is_empty() {
        return Ok(RoaringTreemap::new());
    }
    let mut phrase_matches = RoaringTreemap::new();
    for doc_id in result.iter() {
        if state.tokens_for_doc(doc_id).is_some_and(|tokens| {
            tokens
                .windows(query_tokens.len())
                .any(|w| w == query_tokens)
        }) {
            phrase_matches.insert(doc_id);
        }
    }
    Ok(phrase_matches)
}

fn matching_boolean_query(
    state: &IndexState,
    query: &str,
    config: &FtsConfig,
) -> FtsResult<Option<RoaringTreemap>> {
    let tokens: Vec<&str> = query.split_whitespace().collect();
    let has_operators = tokens.iter().any(|token| {
        token.eq_ignore_ascii_case("OR")
            || token.eq_ignore_ascii_case("AND")
            || token.eq_ignore_ascii_case("NOT")
            || token.starts_with('-')
    });
    if !has_operators {
        return Ok(None);
    }

    let mut groups: Vec<Vec<(bool, &str)>> = vec![Vec::new()];
    let mut negate_next = false;
    for token in tokens {
        if token.eq_ignore_ascii_case("OR") {
            if groups.last().is_some_and(Vec::is_empty) {
                return Err(FtsError::CorruptIndex(
                    "FTS query has OR without a left operand".into(),
                ));
            }
            groups.push(Vec::new());
            negate_next = false;
            continue;
        }
        if token.eq_ignore_ascii_case("AND") {
            continue;
        }
        if token.eq_ignore_ascii_case("NOT") {
            negate_next = true;
            continue;
        }
        if token.contains('*') || token.contains('?') || token.contains(':') {
            return Err(FtsError::CorruptIndex(format!(
                "Unsupported FTS query operator in '{token}'"
            )));
        }
        let prefixed_negative = token.starts_with('-') && token.len() > 1;
        let term = token
            .strip_prefix('-')
            .or_else(|| token.strip_prefix('+'))
            .unwrap_or(token);
        if term.is_empty() {
            return Err(FtsError::CorruptIndex("Empty FTS query term".into()));
        }
        groups
            .last_mut()
            .expect("at least one group")
            .push((negate_next || prefixed_negative, term));
        negate_next = false;
    }
    if negate_next || groups.last().is_some_and(Vec::is_empty) {
        return Err(FtsError::CorruptIndex(
            "FTS query ends with an operator".into(),
        ));
    }

    let mut union = RoaringTreemap::new();
    for group in groups {
        let mut positives: Option<RoaringTreemap> = None;
        let mut negatives = Vec::new();
        for (negative, term) in group {
            let analyzed = analyze_document(term, config);
            let docs = matching_docs(state, &analyzed, false)?;
            if negative {
                negatives.push(docs);
            } else {
                positives = Some(match positives {
                    Some(mut current) => {
                        current &= docs;
                        current
                    }
                    None => docs,
                });
            }
        }
        // A prohibition-only group has no positive universe and therefore
        // produces no matches instead of accidentally returning the forbidden set.
        let mut group_docs = positives.unwrap_or_default();
        for forbidden in negatives {
            group_docs -= forbidden;
        }
        union |= group_docs;
    }
    Ok(Some(union))
}
