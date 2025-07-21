# Data Description for Large Language Models (DDLLM)

TL;DR: Allow LLMs to interface (safely) with arbitrary data via SQLite syntax

## Background

LLMs are great at writing code, including SQL, to accomplish tasks. Using SQL as a
data description and querying format allows a perfect blend of flexibility to service
novel requests, while still being able to enforce "bounds" on the LLM.

There are problems with opening up an existing API or app to this kind of interface though:

1) LLMs are susceptible to prompt injection, so if you're allowing 1:1 access to a database,
   you could run into a data-loss or data-injection scenario.
2) You may need to restrict access to a slice of data (accounts matching some ID, for example)
3) You may not have direct access to a database, but perhaps you have some rest apis, and want
   the power of SQL

This project is an attempt to solve the above generally in a way that is easy to "glue in" to
existing APIs or data layers.

## Installing

TODO

## Examples

TODO


