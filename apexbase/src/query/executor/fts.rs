// FTS query helpers: MATCH()/FUZZY_MATCH() resolution to compressed bitmaps and score projection.

impl ApexExecutor {
    // ========== FTS Helper: resolve MATCH()/FUZZY_MATCH() to a compressed bitmap ==========

    fn ensure_fts_enabled_for_query(base_dir: &Path, table_name: &str) -> io::Result<()> {
        let config = Self::read_fts_config(base_dir);
        let enabled = config
            .get(table_name)
            .and_then(|entry| entry.get("enabled"))
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false);
        if enabled {
            Ok(())
        } else {
            Err(err_data(format!(
                "FTS is disabled for table '{}'",
                table_name
            )))
        }
    }

    fn collect_fts_queries(expr: &SqlExpr, matches: &mut Vec<String>, scores: &mut Vec<Option<String>>) {
        match expr {
            SqlExpr::FtsMatch { query, fuzzy: false } => matches.push(query.clone()),
            SqlExpr::FtsScore { query } => scores.push(query.clone()),
            SqlExpr::BinaryOp { left, right, .. } => {
                Self::collect_fts_queries(left, matches, scores);
                Self::collect_fts_queries(right, matches, scores);
            }
            SqlExpr::UnaryOp { expr, .. }
            | SqlExpr::Paren(expr)
            | SqlExpr::Cast { expr, .. } => Self::collect_fts_queries(expr, matches, scores),
            SqlExpr::Function { args, .. } => {
                for arg in args {
                    Self::collect_fts_queries(arg, matches, scores);
                }
            }
            SqlExpr::Case { when_then, else_expr } => {
                for (when, then) in when_then {
                    Self::collect_fts_queries(when, matches, scores);
                    Self::collect_fts_queries(then, matches, scores);
                }
                if let Some(expr) = else_expr {
                    Self::collect_fts_queries(expr, matches, scores);
                }
            }
            SqlExpr::Between { low, high, .. } => {
                Self::collect_fts_queries(low, matches, scores);
                Self::collect_fts_queries(high, matches, scores);
            }
            SqlExpr::ArrayIndex { array, index } => {
                Self::collect_fts_queries(array, matches, scores);
                Self::collect_fts_queries(index, matches, scores);
            }
            _ => {}
        }
    }

    /// Bound the FTS score map for `ORDER BY FTS_SCORE() DESC LIMIT n`.
    ///
    /// The full map materialises every match; when the statement's first sort
    /// key is a descending FTS score and a LIMIT applies, only the top
    /// `offset + limit` rows can appear, so the FTS engine may return just that
    /// many hits. Anything else (ascending score, other sort keys, grouping,
    /// joins, DISTINCT) keeps the full map.
    fn fts_score_limit(stmt: &SelectStatement) -> Option<usize> {
        if stmt.distinct
            || stmt.distinct_on.is_some()
            || !stmt.joins.is_empty()
            || !stmt.group_by.is_empty()
            || !stmt.group_by_exprs.is_empty()
            || stmt.having.is_some()
        {
            return None;
        }
        let limit = stmt.limit?;
        let first = stmt.order_by.first()?;
        if !first.descending {
            return None;
        }
        match &first.expr {
            Some(SqlExpr::FtsScore { .. }) => {
                Some(limit.saturating_add(stmt.offset.unwrap_or(0)))
            }
            _ => None,
        }
    }

    fn resolve_fts_score_expr(
        expr: SqlExpr,
        default_query: Option<&str>,
        score_maps: &AHashMap<String, Arc<AHashMap<u64, f32>>>,
    ) -> io::Result<SqlExpr> {
        match expr {
            SqlExpr::FtsScore { query } => {
                let query = query
                    .as_deref()
                    .or(default_query)
                    .ok_or_else(|| err_input(
                        "FTS_SCORE() requires exactly one MATCH() query or an explicit string argument",
                    ))?;
                let scores = score_maps
                    .get(query)
                    .cloned()
                    .ok_or_else(|| err_data("FTS score map was not prepared"))?;
                Ok(SqlExpr::FtsScoreResolved { scores })
            }
            SqlExpr::BinaryOp { left, op, right } => Ok(SqlExpr::BinaryOp {
                left: Box::new(Self::resolve_fts_score_expr(*left, default_query, score_maps)?),
                op,
                right: Box::new(Self::resolve_fts_score_expr(*right, default_query, score_maps)?),
            }),
            SqlExpr::UnaryOp { op, expr } => Ok(SqlExpr::UnaryOp {
                op,
                expr: Box::new(Self::resolve_fts_score_expr(*expr, default_query, score_maps)?),
            }),
            SqlExpr::Paren(expr) => Ok(SqlExpr::Paren(Box::new(Self::resolve_fts_score_expr(
                *expr, default_query, score_maps,
            )?))),
            SqlExpr::Function { name, args } => Ok(SqlExpr::Function {
                name,
                args: args
                    .into_iter()
                    .map(|arg| Self::resolve_fts_score_expr(arg, default_query, score_maps))
                    .collect::<io::Result<Vec<_>>>()?,
            }),
            SqlExpr::Cast { expr, data_type } => Ok(SqlExpr::Cast {
                expr: Box::new(Self::resolve_fts_score_expr(*expr, default_query, score_maps)?),
                data_type,
            }),
            other => Ok(other),
        }
    }

    fn resolve_fts_scores_in_statement(
        stmt: &mut SelectStatement,
        base_dir: &Path,
        table_name: &str,
    ) -> io::Result<()> {
        let mut matches = Vec::new();
        let mut requested = Vec::new();
        if let Some(expr) = &stmt.where_clause {
            Self::collect_fts_queries(expr, &mut matches, &mut requested);
        }
        if let Some(expr) = &stmt.having {
            Self::collect_fts_queries(expr, &mut matches, &mut requested);
        }
        for column in &stmt.columns {
            match column {
                SelectColumn::Expression { expr, .. } => {
                    Self::collect_fts_queries(expr, &mut matches, &mut requested)
                }
                SelectColumn::AllReplace(replacements) => {
                    for (expr, _) in replacements {
                        Self::collect_fts_queries(expr, &mut matches, &mut requested);
                    }
                }
                _ => {}
            }
        }
        for order in &stmt.order_by {
            if let Some(expr) = &order.expr {
                Self::collect_fts_queries(expr, &mut matches, &mut requested);
            }
        }
        if requested.is_empty() {
            return Ok(());
        }
        Self::ensure_fts_enabled_for_query(base_dir, table_name)?;

        matches.sort_unstable();
        matches.dedup();
        let default_query = (matches.len() == 1).then(|| matches[0].as_str());
        let mut queries = Vec::with_capacity(requested.len());
        for query in requested {
            let query = query
                .or_else(|| default_query.map(str::to_owned))
                .ok_or_else(|| err_input(
                    "FTS_SCORE() requires exactly one MATCH() query or an explicit string argument",
                ))?;
            queries.push(query);
        }
        queries.sort_unstable();
        queries.dedup();

        let manager = crate::query::executor::get_or_create_fts_manager(base_dir);
        crate::query::executor::wait_fts_backfill(base_dir, table_name);
        let engine = manager
            .get_engine(table_name)
            .map_err(|error| err_data(error.to_string()))?;
        let score_limit = Self::fts_score_limit(stmt);
        let mut score_maps = AHashMap::with_capacity(queries.len());
        for query in queries {
            let hits = match score_limit {
                Some(limit) => engine.search_ranked(&query, limit),
                None => engine.search_scored(&query),
            }
            .map_err(|error| err_data(error.to_string()))?;
            let scores = hits
                .into_iter()
                .map(|hit| (hit.doc_id, hit.score))
                .collect::<AHashMap<_, _>>();
            score_maps.insert(query, Arc::new(scores));
        }

        for column in &mut stmt.columns {
            match column {
                SelectColumn::Expression { expr, .. } => {
                    *expr = Self::resolve_fts_score_expr(expr.clone(), default_query, &score_maps)?;
                }
                SelectColumn::AllReplace(replacements) => {
                    for (expr, _) in replacements {
                        *expr = Self::resolve_fts_score_expr(expr.clone(), default_query, &score_maps)?;
                    }
                }
                _ => {}
            }
        }
        for order in &mut stmt.order_by {
            if let Some(expr) = &mut order.expr {
                *expr = Self::resolve_fts_score_expr(expr.clone(), default_query, &score_maps)?;
            }
        }
        if let Some(expr) = &mut stmt.having {
            *expr = Self::resolve_fts_score_expr(expr.clone(), default_query, &score_maps)?;
        }
        if let Some(expr) = &mut stmt.where_clause {
            *expr = Self::resolve_fts_score_expr(expr.clone(), default_query, &score_maps)?;
        }
        Ok(())
    }

    /// Resolve every FTS node once and retain its compressed Roaring bitmap in the
    /// runtime expression. This avoids allocating one `Value` per hit and lets Arrow
    /// batches test `_id` membership directly.
    fn resolve_fts_in_expr(
        expr: SqlExpr,
        base_dir: &Path,
        table_name: &str,
    ) -> io::Result<SqlExpr> {
        match expr {
            SqlExpr::FtsMatch { query, fuzzy } => {
                Self::ensure_fts_enabled_for_query(base_dir, table_name)?;
                // A query server such as Arrow Flight may not share the Python
                // client's process-local registry. The persisted enablement
                // marker above is authoritative, so lazily load the same index.
                let mgr = crate::query::executor::get_or_create_fts_manager(base_dir);
                crate::query::executor::wait_fts_backfill(base_dir, table_name);
                let engine = mgr
                    .get_engine(table_name)
                    .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
                let result = if fuzzy {
                    engine
                        .fuzzy_search(&query, 1)
                        .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?
                } else {
                    engine
                        .search(&query)
                        .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?
                };
                Ok(SqlExpr::FtsResolved {
                    doc_ids: result.shared_bitmap(),
                })
            }
            SqlExpr::BinaryOp { left, op, right } => Ok(SqlExpr::BinaryOp {
                left: Box::new(Self::resolve_fts_in_expr(*left, base_dir, table_name)?),
                op,
                right: Box::new(Self::resolve_fts_in_expr(*right, base_dir, table_name)?),
            }),
            SqlExpr::UnaryOp { op, expr } => Ok(SqlExpr::UnaryOp {
                op,
                expr: Box::new(Self::resolve_fts_in_expr(*expr, base_dir, table_name)?),
            }),
            SqlExpr::Paren(inner) => Ok(SqlExpr::Paren(Box::new(Self::resolve_fts_in_expr(
                *inner, base_dir, table_name,
            )?))),
            // All other variants have no nested SqlExpr that could contain FtsMatch
            other => Ok(other),
        }
    }

    /// Return true iff `expr` contains at least one `FtsMatch` node.
    fn expr_has_fts_match(expr: &SqlExpr) -> bool {
        match expr {
            SqlExpr::FtsMatch { .. } => true,
            SqlExpr::BinaryOp { left, right, .. } => {
                Self::expr_has_fts_match(left) || Self::expr_has_fts_match(right)
            }
            SqlExpr::UnaryOp { expr, .. } => Self::expr_has_fts_match(expr),
            SqlExpr::Paren(inner) => Self::expr_has_fts_match(inner),
            _ => false,
        }
    }
}
