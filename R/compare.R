#' Build the specifications for CSV writer
#' @param separator The CSV delimiter character.
#' @param headers If TRUE the header line will be written.
#' @param na_value Specifies how missing data will be represented in the CSV file.
#' @return A list with the packed parameters.
#' @export
csv_spec <- function(separator=";", headers=TRUE, na_value="<NA>"){
    list(separator=separator, headers=headers, na_value=na_value)
}

#' Build the specifications for the HTML report
#' @param template Full path of the HTML template.
#' @return A list.
#' @export
html_spec <- function(template=NULL){
    template <- if (is.null(template)){
        system.file("report-template.html", package="sparkdataframecompare")
    } else{
        template
    }
    list(template=template)
}

#' Build the specifications for the output report
#' @param output_directory Where to write the report files.
#' @param report_kind Kind of report that will be generated. one of: text, html or null.
#'   If null, no report is written but the raw data are returned.
#' @param missing_left_limit Limits the number of output rows for data missing in the first table. NA means no limit.
#' @param missing_left_only_keys If TRUE, only keys are written for missing data on the first table, otherwise the full row is written.
#' @param missing_right_limit Limits the number of output rows for data missing in the second table. NA means no limit.
#' @param missing_right_only_keys If TRUE, only keys are written for missing data on the second table, otherwise the full row is written.
#' @param cols_diff_limit The number of rows to be written for each differing column. NULL means no limit, < 0 means no output at all.
#' @param csv_specs A csv_spec object.
#' @param html_specs A html_spec object.
#' @param variables_documentation A named list with documentation for each compared variable.
#' @return A list with the given parameters.
#' @export
report_spec <- function(output_directory, report_kind="text",
                        missing_left_limit=NA, missing_left_only_keys=FALSE,
                        missing_right_limit=NA, missing_right_only_keys=FALSE,
                        cols_diff_limit=NULL, csv_specs=NULL, html_specs=NULL,
                        variables_documentation=NULL){
    report_kinds <- list("html"=html_report, "text"=text_report, "null"=null_report)

    if (!report_kind %in% names(report_kinds)){
        stop(sprintf("Invalid report_kind - should be one of: %s", paste(names(report_kinds), collapse=", ")))
    }

    if (is.null(csv_specs)){
        csv_specs <- csv_spec()
    }

    if (is.null(html_specs)){
        html_specs <- html_spec()
    }

    if (is.null(variables_documentation)){
        variables_documentation <- list()
    }

    list(
        output_directory=output_directory,
        report_fun=report_kinds[[report_kind]],
        missing_left_limit=missing_left_limit,
        missing_left_only_keys=missing_left_only_keys,
        missing_right_limit=missing_right_limit,
        missing_right_only_keys=missing_right_only_keys,
        cols_diff_limit=cols_diff_limit,
        csv_specs=csv_specs,
        html_specs=html_specs,
        variables_documentation=variables_documentation
    )
}

is_local_path <- function(path){
    !grepl(".*://.*", path)
}

join_path <- function(dir, fname){
    if (is_local_path(dir)){
        normalizePath(file.path(dir, fname), mustWork = FALSE)
    } else{
        sprintf("%s/%s", dir, fname)
    }
}

types <- function(df){
    fields <- SparkR::schema(df)$fields()
    field_names <- lapply(fields, function(x){x$name()})
    names(fields) <- field_names
    fields
}

save_csv <- function(df, dirname, out_name, output_path, csv_specs){
    spark_path <- join_path(output_path, dirname)

    SparkR::write.df(SparkR::repartition(df, 1), spark_path, source="csv", mode="overwrite", sep=csv_specs$separator, header=csv_specs$headers, nullValue=csv_specs$na_value)

    if (is_local_path(output_path)){
        csv <- list.files(spark_path, ".*\\.csv$", full.names = TRUE)
        out_file <- join_path(output_path, out_name)

        if (length(csv) == 1){
            if (!file.rename(csv, out_file)){
                stop(sprintf("Can't rename %s to %s", csv, out_file))
            }

            if (unlink(spark_path, recursive=TRUE, force=TRUE) != 0){
                stop(sprintf("Can't remove %s", spark_path))
            }

            list("local"=TRUE, "file"=out_file)
        } else{
            list("local"=FALSE, "file"=spark_path)
        }
    } else{
        list("local"=FALSE, "file"=spark_path)
    }
}

take_sample <- function(df, size){
    pct <- SparkR::count(df) / size * 1.5
    SparkR::limit(SparkR::sample(df, fraction=pct), size)
}

save_missing_rows_file <- function(name, df, output_path, limit, only_keys, key_cols, csv_specs){
    if (is.null(limit)){
        NULL
    } else{


        if (only_keys){
            df <- do.call(SparkR::select, append(df, key_cols))
        }

        df <- SparkR::repartition(df, 1)
        df <- do.call(SparkR::arrange, append(df, key_cols))

        if (!is.na(limit)){
            df <- SparkR::limit(df, limit)
        }

        save_csv(df, sprintf("missing_rows_in_%s_raw.csv", name), sprintf("missing_rows_in_%s.csv", name), output_path, csv_specs)
    }
}

is_numeric_type <- function(t){
    cls_num <- SparkR::sparkR.callJStatic("java.lang.Class", "forName", "org.apache.spark.sql.types.NumericType")
    cls_obj <- SparkR::sparkR.callJMethod(SparkR::sparkR.callJMethod(t$jobj, "dataType"), "getClass")

    SparkR::sparkR.callJMethod(cls_num, "isAssignableFrom", cls_obj)
}

compare_column <- function(df, df1, df2, column_name, table_name_1, table_name_2, spec1, spec2, key_cols, numeric_tolerance=1e-6){
    type1 <- spec1[[column_name]]
    type2 <- spec2[[column_name]]

    col1 <- SparkR::alias(df1[[column_name]], table_name_1)
    col2 <- SparkR::alias(df2[[column_name]], table_name_2)

    cols <- append(lapply(key_cols, function(x){df1[[x]]}), list(col1, col2))

    df <- do.call(SparkR::select, append(df, cols))

    col1 <- df[[table_name_1]]
    col2 <- df[[table_name_2]]

    if (is_numeric_type(type1) && is_numeric_type(type2)){
        # count NAs
        df <- SparkR::mutate(df,
                              "nan_1"=SparkR::when(SparkR::isnan(col1), 1),
                              "null_1"=SparkR::when(SparkR::isNull(col1), 1),
                              "nan_2"=SparkR::when(SparkR::isnan(col2), 1),
                              "null_2"=SparkR::when(SparkR::isNull(col2), 1))
        df <-
            SparkR::mutate(df,
                           NA_1=SparkR::coalesce(df$nan_1, df$null_1, lit(0)),
                           NA_2=SparkR::coalesce(df$nan_2, df$null_2, lit(0)))
        df <-
            SparkR::mutate(df, NA_both=SparkR::ifelse(df$NA_1 + df$NA_2 > lit(1), 1, 0), NA_count=df$NA_1 + df$NA_2)

        na_count <- SparkR::collect(SparkR::summarize(df, NA_1=sum(df$NA_1), NA_2=sum(df$NA_2), NA_both=sum(df$NA_both)))

        df <-
            SparkR::where(df, (df$NA_count == 1) | (df$NA_count == 0 & abs(col1 - col2) > numeric_tolerance))
        df <-
            SparkR::mutate(df, "ABS_DIFF"=SparkR::ifelse(df$NA_count==0, col1 - col2, 0), "REL_DIFF"=SparkR::ifelse(df$NA_count==0, (col1-col2)/col1, 0))
    } else{
        # count NAs
        df <- SparkR::mutate(df,
                              "NA_1"=SparkR::otherwise(SparkR::when(SparkR::isNull(col1), 1), 0),
                              "NA_2"=SparkR::otherwise(SparkR::when(SparkR::isNull(col2), 1), 0),
                              "NA_both"=SparkR::otherwise(SparkR::when(SparkR::isNull(col1) & SparkR::isNull(col2), 1), 0))

        df <- SparkR::mutate(df, "NA_count"=df$NA_1 + df$NA_2)

        na_count <- SparkR::collect(SparkR::summarize(df, NA_1=sum(df$NA_1), NA_2=sum(df$NA_2), NA_both=sum(df$NA_both)))

        df <-
            SparkR::where(df, (df$NA_count == 1) | (df$NA_count == 0 & col1 != col2))
        df <-
            SparkR::mutate(df, "ABS_DIFF"=SparkR::lit(1), "REL_DIFF"=SparkR::lit(1))
    }

    df <- do.call(SparkR::arrange, append(df, lapply(key_cols, function(x){df[[x]]})))
    list(df=df, na=na_count)
}

compare_rows <- function(df, df1, df2, cols, spec1, spec2, key_cols, numeric_tolerance=1e-6){
    cnt_cols <- mapply(function(column_name){
        type1 <- spec1[[column_name]]
        type2 <- spec2[[column_name]]

        col1 <- df1[[column_name]]
        col2 <- df2[[column_name]]

        if (is_numeric_type(type1) && is_numeric_type(type2)){
            col <- SparkR::when(abs(col1 - col2) > numeric_tolerance, SparkR::lit(1))
            col <- SparkR::otherwise(col, SparkR::lit(0))
        } else{
            col <- SparkR::when(col1 != col2, SparkR::lit(1))
            col <- SparkR::otherwise(col, SparkR::lit(0))
        }

        col
    }, cols, SIMPLIFY = FALSE)

    n <- length(cols)

    diff <- SparkR::alias(Reduce(`+`, cnt_cols), "diff")
    df <- SparkR::select(df, diff)
    df <- SparkR::mutate(df, "diff"=df$diff/SparkR::lit(n)*SparkR::lit(100))
    hist <- SparkR::histogram(df, "diff", 20)
    sum <- SparkR::collect(SparkR::summary(df))
    list("histogram"=hist, "summary"=sum)
}

save_diff_col_file <- function(df, name, output_directory, cols_diff_limit, csv_specs){
    if (!is.null(cols_diff_limit) && cols_diff_limit < 0){
        NULL
    } else{
        if (!is.null(cols_diff_limit)){
            df <- SparkR::limit(df, cols_diff_limit)
        }

        save_csv(df, sprintf("%s_dir.csv", name), sprintf("%s.csv", name), output_directory, csv_specs)
    }
}

#' Show the progress of columns comparison on the console
#' @param n The current item index (1-based).
#' @param tot The number of items to be checked.
#' @param name The name of the variable being compared.
#' @param size The length of the output string.
#' @export
text_progress <- function(n, tot, name, size=80){
    if (n <= 1){
        l <- sprintf("| %d  ", n)
        t <- sprintf("  %d |", tot)
        cat(l)
        cat(paste0(rep("-", size - nchar(l) - nchar(t) + 2), collapse=""))
        cat(t)
        cat("\n ")
    }

    f <- (size-1) / tot
    t <- n * f

    if (t - floor(t) <= f){
        cat(paste0(rep("=", ceiling(f)), collapse=""))
    }

    if (n >= tot){
        cat("\n")
    }
}

#' Compare two SparkDataFrames registered as temporary tables.
#' @description Generate a report based on the given specs.
#' A summary report is generated containing differences between the two given tables plus a set of CSV files with per-variable differences.
#' The execution is stopped if no common rows are found by the join_cols.
#' @param table_name_1 Registered name of the left table.
#' @param table_name_2 Registered name of the right table.
#' @param join_cols The character vector of column names on which the two tables will be joined. Requires that the join columns have the same names on both tables.
#' @param report_spec Specifications for the output report.
#' @param exclude_cols Character vector of columns to be excluded from the comparison.
#' @param include_cols Character vector of columns to be checked.
#' @param logger A function for progress reporting - default to print, NULL to disable output.
#' @param progress_fun A function for per-column progress - see text_progress for signature.
#' @export
compare <- function(table_name_1, table_name_2, join_cols, report_spec, exclude_cols=NULL, include_cols=NULL, logger=print, progress_fun=text_progress){
    if (is.null(logger)){
        logger <- function(...){invisible(NULL)}
    }

    if (is.null(progress_fun)){
        progress_fun <- function(...){invisible(NULL)}
    }

    if (!is.character(join_cols)) {
        stop("join_spec must be a vector of column names")
    }

    df1 <- SparkR::tableToDF(table_name_1)
    df2 <- SparkR::tableToDF(table_name_2)

    # always exclude join columns from comparison
    exclude_cols <-
        if (is.null(exclude_cols)){
            join_cols
        } else{
            base::union(join_cols, exclude_cols)
        }

    cols1 <- setdiff(SparkR::colnames(df1), exclude_cols)
    cols2 <- setdiff(SparkR::colnames(df2), exclude_cols)

    cols_missing_in_1 <- setdiff(cols2, cols1)
    cols_missing_in_2 <- setdiff(cols1, cols2)

    common_cols <- sort(base::intersect(SparkR::colnames(df1), SparkR::colnames(df2)))

    missing_join_cols <- setdiff(join_cols, common_cols)
    if (length(missing_join_cols) > 0){
        stop(sprintf("Join columns must be in both tables - missing are: %s", paste(missing_join_cols, collapse=", ")))
    }

    # columns to compare taking into account include_cols and exclude_cols
    if (!is.null(include_cols)){
        missing_include <- setdiff(include_cols, common_cols)
        if (length(missing_include) > 0){
            stop(sprintf("include_cols contains invalid columns: %s", paste0(missing_include, collapse=", ")))
        }
        compare_columns <- include_cols
    } else{
        compare_columns <- common_cols
    }
    compare_columns <- setdiff(compare_columns, exclude_cols)


    join_spec <-
        lapply(join_cols, function(x) {
            df1[[x]] == df2[[x]]
        })
    join_spec <- Reduce(`&`, join_spec)

    rows_missing_in_1 <- SparkR::join(df2, df1, join_spec, "left_anti")
    rows_missing_in_2 <- SparkR::join(df1, df2, join_spec, "left_anti")

    rows_common <-
        SparkR::join(df1, df2, join_spec, "inner")
    rows_common <-
        do.call(SparkR::arrange, append(rows_common, lapply(join_cols, function(x){df1[[x]]})))

    SparkR::persist(rows_common, "MEMORY_AND_DISK_2")

    logger("Analysing common rows")
    common_rows_count <- SparkR::count(rows_common)
    if (common_rows_count == 0){
        stop("No common rows with the given join keys")
    }

    logger(sprintf("Analysing missing rows from %s", table_name_1))
    missing_rows_1_file <-
        save_missing_rows_file(
            table_name_1,
            rows_missing_in_1,
            report_spec$output_directory,
            report_spec$missing_left_limit,
            report_spec$missing_left_only_keys,
            lapply(join_cols, function(x){df2[[x]]}),
            report_spec$csv_specs)

    logger(sprintf("Analysing missing rows from %s", table_name_2))
    missing_rows_2_file <-
        save_missing_rows_file(
            table_name_2,
            rows_missing_in_2,
            report_spec$output_directory,
            report_spec$missing_right_limit,
            report_spec$missing_right_only_keys,
            lapply(join_cols, function(x){df1[[x]]}),
            report_spec$csv_specs)

    logger("Analysing differences between columns")

    spec1 <- types(df1)
    spec2 <- types(df2)

    diff_cols <-
        mapply(function(x, i){
            progress_fun(i, length(compare_columns), x)

            res <- compare_column(rows_common, df1, df2, x, table_name_1, table_name_2, spec1, spec2, join_cols)
            df <- res$df
            SparkR::persist(df, "MEMORY_ONLY_2")

            summary <- SparkR::collect(SparkR::summary(SparkR::select(df, "ABS_DIFF", "REL_DIFF")))

            abs_diff <- as.list(as.double(summary$ABS_DIFF))
            names(abs_diff) <- summary$summary
            rel_diff <- as.list(as.double(summary$REL_DIFF))
            names(rel_diff) <- summary$summary

            if (abs_diff$count > 0){
                file <- save_diff_col_file(df, x, report_spec$output_directory, report_spec$cols_diff_limit, report_spec$csv_specs)
            } else{
                file <- NULL
            }

            if (is.null(file)){
                file_local <- as.logical(NA)
                file_name <- NA_character_
            } else{
                file_local <- file$local
                file_name <- file$file
            }

            SparkR::unpersist(df, FALSE)

            list(
                "name"=x,
                "type_1"=(spec1[[x]]$dataType.simpleString()),
                "type_2"=(spec2[[x]]$dataType.simpleString()),
                "differences"=as.integer(abs_diff$count),
                "differences_pct"=abs_diff$count / common_rows_count * 100,
                "rel_diff_mean"=rel_diff$mean,
                "rel_diff_stddev"=rel_diff$stddev,
                "rel_diff_min"=rel_diff$min,
                "rel_diff_p25"=rel_diff$`25%`,
                "rel_diff_p50"=rel_diff$`50%`,
                "rel_diff_p75"=rel_diff$`75%`,
                "rel_diff_max"=rel_diff$max,
                "abs_diff_mean"=abs_diff$mean,
                "abs_diff_stddev"=abs_diff$stddev,
                "abs_diff_min"=abs_diff$min,
                "abs_diff_p25"=abs_diff$`25%`,
                "abs_diff_p50"=abs_diff$`50%`,
                "abs_diff_p75"=abs_diff$`75%`,
                "abs_diff_max"=abs_diff$max,
                "NA_1"=res$na$NA_1,
                "NA_2"=res$na$NA_2,
                "NA_both"=res$na$NA_both,
                "file_local"=file_local,
                "file_name"=file_name,
                "has_differences"=abs_diff$count > 0
            )

        }, compare_columns, seq_along(compare_columns), SIMPLIFY = FALSE, USE.NAMES = FALSE)

    # generate a data frame
    diff_cols <- Reduce(rbind, Map(function(x){data.frame(x, stringsAsFactors = FALSE, row.names = FALSE)}, diff_cols))
    diff_cols <- diff_cols[order(diff_cols$name),]

    # differences count by row
    logger("Analysing differences by row")
    rd <- compare_rows(rows_common, df1, df2, compare_columns, spec1, spec2, join_cols)

    SparkR::unpersist(rows_common)

    raw_data <-
        list(
            "table_1"=table_name_1,
            "table_2"=table_name_2,
            "join_cols"=join_cols,
            "rows"=list(
                "missing_in_1"=list(
                    "count"=SparkR::count(rows_missing_in_1),
                    "file"=missing_rows_1_file
                ),
                "missing_in_2"=list(
                    "count"=SparkR::count(rows_missing_in_2),
                    "file"=missing_rows_2_file
                ),
                "common"=list("count"=common_rows_count),
                "differences"=rd
            ),
            "columns"=list(
                "missing_in_1"=list("count"=length(cols_missing_in_1), "names"=sort(cols_missing_in_1)),
                "missing_in_2"=list("count"=length(cols_missing_in_2), "names"=sort(cols_missing_in_2)),
                "common"=list("count"=length(common_cols), "names"=sort(common_cols)),
                "compare_columns"=compare_columns,
                "diff"=diff_cols
            )
        )

    saveRDS(raw_data, join_path(report_spec$output_directory, "raw-report-data.rds"))
    report_spec$report_fun(raw_data, report_spec)
}

