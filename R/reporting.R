html_report <- function(data, report_spec){
    render_tag <- function(x){
        attrs <-
            if (!is.null(x$attributes)){
                mapply(function(x, n){
                    sprintf("%s=\"%s\"", n, gsub("\"", "&quot;", x, fixed=TRUE))
                }, x$attributes, names(x$attributes), SIMPLIFY = FALSE)
            } else{
                ""
            }

        attrs <- paste0(attrs, collapse=" ")

        sprintf("<%s %s>%s</%s>", x$tag, attrs, x$value, x$tag)
    }

    render_table <- function(df, class=""){
        headers <- names(df)

        gen_item <- function(x, tag){
            if (is.list(x)){
                render_tag(list("tag"=tag, "value"=render_tag(x[[1]])))

            } else{
                cls <- if (is.na(x)) "null" else if (is.numeric(x)) "has-text-right" else ""
                v <- if (is.na(x)) "n/a" else if (is.integer(x)) sprintf("%d", x) else if (is.numeric(x)) sprintf("%.2f", x) else sprintf("%s", x)
                t <- if (is.numeric(x)) as.character(x) else ""
                render_tag(list("tag"=tag, "attributes"=list("class"=cls, "title"=t), "value"=v))
            }
        }

        gen_row <- function(row, tag){
            r <- mapply(function(x) gen_item(x, tag), row)
            sprintf("<tr>%s</tr>", paste0(unlist(r), collapse="\n"))
        }

        tmp <- by(df, 1:nrow(df), function(x) gen_row(x, "td"), simplify = TRUE)

        sprintf("<div class=\"table-fullwidth\"><table class=\"%s\"><thead>%s</thead><tbody>%s</tbody></table></div>", class, gen_row(headers, "th"), paste0(tmp, collapse="\n"))
    }

    render_list <- function(x){
        items <- mapply(function(x){
            sprintf("<li>%s</li>", x)
        }, x, SIMPLIFY = FALSE)
        sprintf("<ul>%s</ul>", paste0(items, collapse="\n"))
    }

    data <- rlang::duplicate(data)

    data$columns$diff <- data$columns$diff[order(data$columns$diff$differences, decreasing = TRUE),]
    data$columns$diff$name <- mapply(function(x, n){
        if (is.na(n)){
            list("tag"="span", "value"=x)
        } else{
            attrs <- list("href"=n)
            if (!is.null(report_spec$variables_documentation[[x]])){
                attrs[["title"]] <- report_spec$variables_documentation[[x]]
            }
            list("tag"="a", "attributes"=attrs, "value"=x)
        }
    }, data$columns$diff$name, data$columns$diff$file_name, SIMPLIFY = FALSE, USE.NAMES = FALSE)
    data$columns$diff$file_name <- NULL
    data$columns$diff$file_local <- NULL

    data[["report_time"]] <- Sys.time()
    data[["diff_cnt"]] <- length(data$columns$diff$has_differences[data$columns$diff$has_differences == TRUE])
    data[["render_table"]] <- render_table

    summary <- as.data.frame(t(as.double(data$rows$differences$summary$diff)))
    names(summary) <- data$rows$differences$summary$summary

    text_y <- max(data$rows$differences$histogram$counts)*0.50
    text_inc <- text_y / 15

    plot <-
        ggplot2::ggplot(data$rows$differences$histogram, ggplot2::aes(x = centroids, y = counts)) +
        ggplot2::geom_bar(stat = "identity", show.legend = FALSE, color="lightblue", fill="lightblue") +
        ggplot2::xlab("Difference %") +
        ggplot2::ylab("Frequency") +
        ggplot2::theme(panel.background = ggplot2::element_blank(), panel.grid = ggplot2::element_line(color="grey")) +
        ggplot2::geom_vline(ggplot2::aes(xintercept=min), summary, color="green") +
        ggplot2::geom_text(ggplot2::aes(x=min, y=text_y, label="min"), summary, size=3, angle=90, vjust=-0.4, hjust=0) +
        ggplot2::geom_vline(ggplot2::aes(xintercept=max), summary, color="red") +
        ggplot2::geom_text(ggplot2::aes(x=max, y=text_y+text_inc, label="max"), summary, size=3, angle=90, vjust=-0.4, hjust=0) +
        ggplot2::geom_vline(ggplot2::aes(xintercept=mean), summary, color="blue") +
        ggplot2::geom_text(ggplot2::aes(x=mean, y=text_y+text_inc*3, label="mean"), summary, size=3, angle=90, vjust=-0.4, hjust=0) +
        ggplot2::annotate("rect", xmin=summary$mean-summary$stddev, xmax=summary$mean+summary$stddev, ymin=0, ymax=Inf, alpha=0.1, fill="blue") +
        ggplot2::geom_vline(ggplot2::aes(xintercept=`25%`), summary, color="gray") +
        ggplot2::geom_text(ggplot2::aes(x=`25%`, y=text_y+text_inc*6, label="25% percentile"), summary, size=3, angle=90, vjust=-0.4, hjust=0) +
        ggplot2::geom_vline(ggplot2::aes(xintercept=`50%`), summary, color="black") +
        ggplot2::geom_text(ggplot2::aes(x=`50%`, y=text_y+text_inc*9, label="median"), summary, size=3, angle=90, vjust=-0.4, hjust=0) +
        ggplot2::geom_vline(ggplot2::aes(xintercept=`75%`), summary, color="gray") +
        ggplot2::geom_text(ggplot2::aes(x=`75%`, y=text_y+text_inc*12, label="75% percentile"), summary, size=3, angle=90, vjust=-0.4, hjust=0)


    histogram_path <- join_path(report_spec$output_directory, "rows-histogram.png")
    ggplot2::ggsave(histogram_path, plot)
    data[["rows"]][["histogram"]] <- histogram_path

    file_conn <- file(report_spec$html_specs$template, "r")
    tryCatch({
        tpl <- paste0(readLines(file_conn), collapse="\n")
    }, finally=close(file_conn))

    # poor man's templating - uncorrect but I don't want to add a dependency
    m <- gregexpr("\\${.*?}", tpl, perl=TRUE)
    tags <- unique(regmatches(tpl, m)[[1]])

    subs <-
        mapply(function(x){
            e <- substr(x, 3, nchar(x)-1)
            eval(parse(text=e), envir=data)
        }, tags, SIMPLIFY = FALSE)

    for (n in names(subs)){
        tpl <- gsub(n, subs[[n]], tpl, fixed=TRUE)
    }

    file_conn <- file(join_path(report_spec$output_directory, "report.html"), "w")
    tryCatch({
        writeLines(tpl, file_conn)
    }, finally=close(file_conn))
}


text_report <- function(data, report_spec){
    render_table <- function(df){
        col_lengths <- lapply(df, function(x){max(nchar(as.character(x)), na.rm = TRUE)})
        header_lengths <- nchar(names(df))
        lengths <- as.list(pmax.int(unlist(col_lengths), header_lengths))
        names(lengths) <- names(col_lengths)
        headers <- names(df)
        names(headers) <- names(df)
        types <- as.list(sapply(df, typeof))
        sep_line <- mapply(function(n){
            if (types[[n]] %in% c("double", "integer")){
                sprintf("%s:", paste0(rep("-", lengths[[n]]-1), collapse=""))
            } else{
                paste0(rep("-", lengths[[n]]), collapse="")
            }

        }, headers)

        gen_item <- function(x, n){
            tspec <- if (is.integer(x)) "d" else if (is.numeric(x)) ".2f" else "s"
            spec <- paste0(" %", (if (is.numeric(x)) "" else "-"), lengths[[n]], tspec, " ")
            sprintf(spec, x)
        }

        gen_row <- function(row){
            r <- mapply(gen_item, row, names(row))
            sprintf("|%s|", paste0(unlist(r), collapse="|"))
        }

        tmp <- by(df, 1:nrow(df), gen_row, simplify = TRUE)

        paste0(unlist(append(append(gen_row(headers), gen_row(sep_line)), tmp)), collapse="\n")
    }

    file_conn <- file(join_path(report_spec$output_directory, "report.md"))
    tryCatch({
        title <- sprintf("Differences between <%s> and <%s>", data$table_1, data$table_2)

        diff_cnt <- length(data$columns$diff$has_differences[data$columns$diff$has_differences == TRUE])

        lines <- c(
            title,
            paste0(replicate(nchar(title), "="), collapse=""),
            "",
            "## Informations",
            sprintf("Generated on %s", Sys.time()),
            "",
            sprintf("First table name is: **%s**  ", data$table_1),
            sprintf("Second table name is: **%s**  ", data$table_2),
            sprintf("Tables are matched by the following columns: %s", paste0(unlist(mapply(function(x) sprintf("*%s*", x), data$join_cols)), collapse=", ")),
            "",
            "The tolerance for difference between numeric columns is 10^-6",
            "",
            "",
            "## Summary",
            sprintf("**%d** rows are available in *%s* but missing in *%s*  ", data$rows$missing_in_1$count, data$table_2, data$table_1),
            sprintf("**%d** rows are available in *%s* but missing in *%s*  ", data$rows$missing_in_2$count, data$table_1, data$table_2),
            sprintf("**%d** rows are available in both tables  ", data$rows$common$count),
            "",
            "",
            sprintf("**%d** columns are available in *%s* but missing in *%s*  ", data$columns$missing_in_1$count, data$table_2, data$table_1),
            sprintf("**%d** columns are available in *%s* but missing in *%s*  ", data$columns$missing_in_2$count, data$table_1, data$table_2),
            sprintf("**%d** columns are common to both tables  ", data$columns$common$count),
            sprintf("**%d** columns must be checked  ", length(data$columns$compare_columns)),
            sprintf("**%d** columns have at least a difference between the two tables, while **%d** are identical", diff_cnt, length(data$columns$compare_columns) - diff_cnt),
            "",
            "",
            "## Details",
            "",
            sprintf("#### Columns missing in *%s*", data$table_1),
            paste0(mapply(function(x){sprintf(" * %s", x)}, data$columns$missing_in_1$names), collapse="\n"),
            "",
            sprintf("#### Columns missing from *%s*", data$table_2),
            paste0(mapply(function(x){sprintf(" * %s", x)}, data$columns$missing_in_2$names), collapse="\n"),
            "",
            "#### Column summary",
            render_table(data$columns$diff[order(data$columns$diff$differences, decreasing = TRUE),])

        )

        writeLines(lines, file_conn)

    }, finally=close(file_conn))
}

null_report <- function(data, report_spec){
    data
}
