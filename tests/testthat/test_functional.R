context("Functional end-to-end tests")

tmp_a <- tempfile()
tmp_b <- tempfile()

setup({
    # Try to initialize sparkr. Requires the env var SPARK_HOME pointing to a local spark installation
    if (!is.na(Sys.getenv("SPARK_HOME", unset = NA))){
        library(SparkR, lib.loc=normalizePath(file.path(Sys.getenv("SPARK_HOME"), "R", "lib")))
    } else {
        stop("please set SPARK_HOME env var")
    }

    SparkR::sparkR.session()
})


test_that("Test comparison",{
    library(sparkdataframecompare)

    df_a <- data.frame(
        col_1 = c(2, 3, 4, 5, 6, 7, 8, 9, 10, 11),
        col_2 = as.Date(c("2019-01-01", "2018-12-31", "2018-11-30", "2018-10-31", "2018-09-30", "2018-07-31", "2018-06-30", "2018-01-01", "2017-12-31", "2016-03-31")),
        col_3 = c("v1", "v2", "v3", "v4", "v5", "v6", "v7", "v8", "v9", "v10"),
        col_4 = c(1.1, 2.2, 3.3, 4.4, 5.5, 6.6, 7.7, 8.8, 9.9, 10.0),
        col_5 = c(10L, 20L, 30L, 40L, 50L, 60L, 70L, 80L, 90L, 100L),
        col_extra_a = c("a", "b", "c", "d", "e", "f", "g", "h", "i", "l")
    )

    df_b <- data.frame(
        col_1 = c(2, 3, 4, 5, 6, 7, 12, 9, 10, 11),
        col_2 = as.Date(c("2019-01-01", "2018-12-31", "2018-11-30", "2018-10-31", "2018-08-30", "2018-07-31", "2018-06-30", "2018-01-01", "2017-12-31", "2016-03-31")),
        col_3 = c("v1", "v2", "vv", "v4", "v5", "v6", "v7", "v8", "v9", "v10"),
        col_4 = c(1.1, 2.2, 3.3, 4.4, 5.5, 0.0, 7.7, 8.8, 9.9, 10.0),
        col_5 = c(10L, 40L, 30L, 40L, 50L, 60L, 70L, 80L, 90L, 100L),
        col_extra_b = c("a", "b", "c", "d", "e", "f", "g", "h", "i", "l")
    )

    SparkR::createOrReplaceTempView(SparkR::createDataFrame(df_a), "df_a")
    SparkR::createOrReplaceTempView(SparkR::createDataFrame(df_b), "df_b")

    res <- compare("df_a", "df_b", c("col_1"), report_spec(".", "null"), logger=NULL, progress_fun=NULL)

    expect_equal(res$table_1, "df_a")
    expect_equal(res$table_2, "df_b")
    expect_equal(res$join_cols, c("col_1"))

    expect_equal(res$rows$missing_in_1$count, 1)
    expect_equal(res$rows$missing_in_2$count, 1)
    expect_equal(res$rows$common$count, 9)

    expect_equal(res$columns$missing_in_1, list(count=1, names=c("col_extra_b")))
    expect_equal(res$columns$missing_in_2, list(count=1, names=c("col_extra_a")))
    expect_equal(res$columns$common, list(count=5, names=c("col_1", "col_2", "col_3", "col_4", "col_5")))
    expect_equal(res$columns$compare_columns, c("col_2", "col_3", "col_4", "col_5"))

    col_diff <- res$columns$diff

    # col_2
    df <- col_diff[col_diff$name=="col_2",]
    expect_equal(df$type_1, df$type_2)
    expect_equal(df$type_1, "date")
    expect_equal(df$differences, 1)
    expect_equal(df$differences_pct, 1/9*100)
    expect_equal(df$rel_diff_mean, 1)
    # col_3
    df <- col_diff[col_diff$name=="col_3",]
    expect_equal(df$type_1, df$type_2)
    expect_equal(df$type_1, "string")
    expect_equal(df$differences, 1)
    expect_equal(df$differences_pct, 1/9*100)
    expect_equal(df$rel_diff_mean, 1)
    # col_4
    df <- col_diff[col_diff$name=="col_4",]
    expect_equal(df$type_1, df$type_2)
    expect_equal(df$type_1, "double")
    expect_equal(df$differences, 1)
    expect_equal(df$differences_pct, 1/9*100)
    expect_equal(df$abs_diff_mean, 6.6)
    # col_5
    df <- col_diff[col_diff$name=="col_5",]
    expect_equal(df$type_1, df$type_2)
    expect_equal(df$type_1, "int")
    expect_equal(df$differences, 1)
    expect_equal(df$differences_pct, 1/9*100)
    expect_equal(df$abs_diff_mean, -20)
    expect_equal(df$rel_diff_mean, -1)
})

test_that("Test NA count",{
    library(sparkdataframecompare)


    df_a <- data.frame(
        col_1 = c(1, 2, 3, 4, 5),
        col_2 = as.Date(c("2019-01-01", "2018-12-31", NA, "2018-10-31", "2018-09-30")),
        col_3 = c("v1", "v2", "v3", "v4", NA),
        col_4 = c(NaN, 2.2, 3.3, 4.4, 5.5),
        col_5 = c(NA, 20L, NaN, 40L, 50L)
    )

    df_b <- data.frame(
        col_1 = c(1, 2, 3, 4, 5),
        col_2 = as.Date(c("2019-01-01", NA, "2018-12-31", "2018-10-31", "2018-09-30")),
        col_3 = c("v1", "v2", "v3", "v4", NA),
        col_4 = c(2.2, NaN, 3.3, 4.4, 5.5),
        col_5 = c(NA, NA, NA, NA, 50L)
    )

    # unfortunately as of spark 2.3.2 serialization of NA doesn't work properly, so we need
    # to serialize to CSV and read from CSV in spark
    write.csv(df_a, tmp_a, row.names = FALSE)
    write.csv(df_b, tmp_b, row.names = FALSE)

    schema <- SparkR::structType("col_1 int, col_2 date, col_3 string, col_4 double, col_5 int")

    SparkR::createOrReplaceTempView(SparkR::read.df(tmp_a, source="csv", sep=",", header=TRUE, schema=schema), "df_a")
    SparkR::createOrReplaceTempView(SparkR::read.df(tmp_b, source="csv", sep=",", header=TRUE, schema=schema), "df_b")

    res <- compare("df_a", "df_b", c("col_1"), report_spec(".", "null"), logger=NULL, progress_fun=NULL)

    col_diff <- res$columns$diff

    # col_2
    df <- col_diff[col_diff$name=="col_2",]
    expect_equal(df$type_1, df$type_2)
    expect_equal(df$type_1, "date")
    expect_equal(df$NA_1, 1)
    expect_equal(df$NA_2, 1)
    expect_equal(df$NA_both, 0)
    # col_3
    df <- col_diff[col_diff$name=="col_3",]
    expect_equal(df$type_1, df$type_2)
    expect_equal(df$type_1, "string")
    expect_equal(df$NA_1, 1)
    expect_equal(df$NA_2, 1)
    expect_equal(df$NA_both, 1)
    # col_4
    df <- col_diff[col_diff$name=="col_4",]
    expect_equal(df$type_1, df$type_2)
    expect_equal(df$type_1, "double")
    expect_equal(df$NA_1, 1)
    expect_equal(df$NA_2, 1)
    expect_equal(df$NA_both, 0)
    # col_5
    df <- col_diff[col_diff$name=="col_5",]
    expect_equal(df$type_1, df$type_2)
    expect_equal(df$type_1, "int")
    expect_equal(df$NA_1, 2)
    expect_equal(df$NA_2, 4)
    expect_equal(df$NA_both, 2)
})

teardown({
    SparkR::sparkR.stop()
    unlink(tmp_a)
    unlink(tmp_b)
})
