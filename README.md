
`fileplyr`
==========

> plyr for CSV files

Perform chunkwise processing or split-apply-combine on data in a delimited file(example: CSV) across multiple cores of a single machine with low memory footprint. These functions are a convenient wrapper over the versatile package 'datadr'.

Examples
--------

Load `fileplyr` package

``` r
library("fileplyr")
```

    ## Loading required package: tibble

-   split-apply-combine example

``` r
write.table(mtcars, "mtcars.csv", row.names = FALSE, sep = ",")
temp <- fileply(file     = "mtcars.csv"
             , groupby = c("carb", "gear")
             , fun     = identity
             , collect = "list"
             , sep     =  ","
             , header  = TRUE
             )
```

    ## ----

    ## Job ID: grucoqnlid__2017-02-03_17_58_43

    ## Reading data ...

    ## Completed in 0 secs

    ## Aggregating ...

    ## Completed in 0.7 secs

    ## Collecting ...

    ## Completed in 0.3 secs

    ## Done!

    ## ----

``` r
temp
```

    ## [[1]]
    ## $key
    ## [1] "carb=2|gear=3"
    ## 
    ## $value
    ##    mpg cyl disp  hp drat    wt  qsec vs am gear carb
    ## 1 18.7   8  360 175 3.15 3.440 17.02  0  0    3    2
    ## 2 15.5   8  318 150 2.76 3.520 16.87  0  0    3    2
    ## 3 15.2   8  304 150 3.15 3.435 17.30  0  0    3    2
    ## 4 19.2   8  400 175 3.08 3.845 17.05  0  0    3    2
    ## 
    ## [[2]]
    ## $key
    ## [1] "carb=3|gear=3"
    ## 
    ## $value
    ##    mpg cyl  disp  hp drat   wt qsec vs am gear carb
    ## 1 16.4   8 275.8 180 3.07 4.07 17.4  0  0    3    3
    ## 2 17.3   8 275.8 180 3.07 3.73 17.6  0  0    3    3
    ## 3 15.2   8 275.8 180 3.07 3.78 18.0  0  0    3    3
    ## 
    ## [[3]]
    ## $key
    ## [1] "carb=8|gear=5"
    ## 
    ## $value
    ##   mpg cyl disp  hp drat   wt qsec vs am gear carb
    ## 1  15   8  301 335 3.54 3.57 14.6  0  1    5    8
    ## 
    ## [[4]]
    ## $key
    ## [1] "carb=4|gear=4"
    ## 
    ## $value
    ##    mpg cyl  disp  hp drat    wt  qsec vs am gear carb
    ## 1 21.0   6 160.0 110 3.90 2.620 16.46  0  1    4    4
    ## 2 21.0   6 160.0 110 3.90 2.875 17.02  0  1    4    4
    ## 3 19.2   6 167.6 123 3.92 3.440 18.30  1  0    4    4
    ## 4 17.8   6 167.6 123 3.92 3.440 18.90  1  0    4    4
    ## 
    ## [[5]]
    ## $key
    ## [1] "carb=6|gear=5"
    ## 
    ## $value
    ##    mpg cyl disp  hp drat   wt qsec vs am gear carb
    ## 1 19.7   6  145 175 3.62 2.77 15.5  0  1    5    6
    ## 
    ## [[6]]
    ## $key
    ## [1] "carb=2|gear=4"
    ## 
    ## $value
    ##    mpg cyl  disp  hp drat    wt  qsec vs am gear carb
    ## 1 24.4   4 146.7  62 3.69 3.190 20.00  1  0    4    2
    ## 2 22.8   4 140.8  95 3.92 3.150 22.90  1  0    4    2
    ## 3 30.4   4  75.7  52 4.93 1.615 18.52  1  1    4    2
    ## 4 21.4   4 121.0 109 4.11 2.780 18.60  1  1    4    2
    ## 
    ## [[7]]
    ## $key
    ## [1] "carb=1|gear=4"
    ## 
    ## $value
    ##    mpg cyl  disp hp drat    wt  qsec vs am gear carb
    ## 1 22.8   4 108.0 93 3.85 2.320 18.61  1  1    4    1
    ## 2 32.4   4  78.7 66 4.08 2.200 19.47  1  1    4    1
    ## 3 33.9   4  71.1 65 4.22 1.835 19.90  1  1    4    1
    ## 4 27.3   4  79.0 66 4.08 1.935 18.90  1  1    4    1
    ## 
    ## [[8]]
    ## $key
    ## [1] "carb=4|gear=5"
    ## 
    ## $value
    ##    mpg cyl disp  hp drat   wt qsec vs am gear carb
    ## 1 15.8   8  351 264 4.22 3.17 14.5  0  1    5    4
    ## 
    ## [[9]]
    ## $key
    ## [1] "carb=2|gear=5"
    ## 
    ## $value
    ##    mpg cyl  disp  hp drat    wt qsec vs am gear carb
    ## 1 26.0   4 120.3  91 4.43 2.140 16.7  0  1    5    2
    ## 2 30.4   4  95.1 113 3.77 1.513 16.9  1  1    5    2
    ## 
    ## [[10]]
    ## $key
    ## [1] "carb=4|gear=3"
    ## 
    ## $value
    ##    mpg cyl disp  hp drat    wt  qsec vs am gear carb
    ## 1 14.3   8  360 245 3.21 3.570 15.84  0  0    3    4
    ## 2 10.4   8  472 205 2.93 5.250 17.98  0  0    3    4
    ## 3 10.4   8  460 215 3.00 5.424 17.82  0  0    3    4
    ## 4 14.7   8  440 230 3.23 5.345 17.42  0  0    3    4
    ## 5 13.3   8  350 245 3.73 3.840 15.41  0  0    3    4
    ## 
    ## [[11]]
    ## $key
    ## [1] "carb=1|gear=3"
    ## 
    ## $value
    ##    mpg cyl  disp  hp drat    wt  qsec vs am gear carb
    ## 1 21.4   6 258.0 110 3.08 3.215 19.44  1  0    3    1
    ## 2 18.1   6 225.0 105 2.76 3.460 20.22  1  0    3    1
    ## 3 21.5   4 120.1  97 3.70 2.465 20.01  1  0    3    1

``` r
to_tibble(temp)
```

    ## # A tibble: 11 × 3
    ##     carb  gear                 value
    ##    <chr> <chr>                <list>
    ## 1      2     3 <data.frame [4 × 11]>
    ## 2      3     3 <data.frame [3 × 11]>
    ## 3      8     5 <data.frame [1 × 11]>
    ## 4      4     4 <data.frame [4 × 11]>
    ## 5      6     5 <data.frame [1 × 11]>
    ## 6      2     4 <data.frame [4 × 11]>
    ## 7      1     4 <data.frame [4 × 11]>
    ## 8      4     5 <data.frame [1 × 11]>
    ## 9      2     5 <data.frame [2 × 11]>
    ## 10     4     3 <data.frame [5 × 11]>
    ## 11     1     3 <data.frame [3 × 11]>

``` r
unlink("mtcars.csv")
```

-   chunkwise processing example

``` r
write.table(mtcars, "mtcars.csv", row.names = FALSE, sep = ",")
temp <- fileply(file     = "mtcars.csv"
             , chunk   = 10
             , fun     = function(x){list(nrow(x))}
             , collect = "dataframe"
             , sep     =  ","
             , header  = TRUE
             )
```

    ## ----

    ## Job ID: nrohgsxilb__2017-02-03_17_58_44

    ## Reading data ...

    ## Completed in 0 secs

    ## Aggregating ...

    ## Completed in 0.7 secs

    ## Collecting ...

    ## Completed in 0.2 secs

    ## Done!

    ## ----

``` r
temp
```

    ##   V1
    ## 1 10
    ## 2  2
    ## 3 10
    ## 4 10

``` r
unlink("mtcars.csv")
```
