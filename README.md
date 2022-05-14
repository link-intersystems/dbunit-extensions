
![Java CI with Maven](https://github.com/link-intersystems/dbunit-extensions/workflows/Java%20CI%20with%20Maven/badge.svg)
[![Coverage Status](https://coveralls.io/repos/github/link-intersystems/dbunit-extensions/badge.svg?branch=master)](https://coveralls.io/github/link-intersystems/dbunit-extensions?branch=master)


# DBUnit compatibility 

The dbunit compatibility tests, test this library against dbunit versions 2.4.6 - 2.7.3.

![DBUnit Compatibility Test](https://github.com/link-intersystems/dbunit-extensions/workflows/DBUnit%20Compatibility%20Tests/badge.svg)

All modules in this project are at least compatible with the dbunit versions within the tested range. 
Limitations and exceptions are listed below.

## lis-dbunit-beans

lis-dbunit-beans needs at least dbunit version 2.2, 
but not all features will properly work.

- Support for BigInteger bean property types does not work.
- TypeConversionException.getCause will always be null. You must use TypeConversionException.getException.
- slf4j api version 1.4.3 must be added.

## lis-dbunit-util

lis-dbunit-util needs at least dbunit version 2.3.0.

- slf4j api version 1.4.3 must be added.





