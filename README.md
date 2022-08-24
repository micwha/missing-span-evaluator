# Lightstep Missing Span Evaluator

This is POC that will evaluate all the traces from a given Stream to find ones that are missing spans. Certain values are currently hardcoded but would later be parameterized. The span analysis is written to a JSON file in the current directory and the traces with missing spans are written to stdout

