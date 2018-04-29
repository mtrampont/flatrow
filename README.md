This little project is an experimentation on type classes and macros.

The idea came from the recurring need of having to "flatten" data models when using Spark dataframe API.

Most of the time the same logic is applied when flattening your objects model.
After trying to make a default flattener using pattern matching and not being satisfied with the result,
I thought this was a good use case to use typeclasses and try to generate a default implementation using macros.

