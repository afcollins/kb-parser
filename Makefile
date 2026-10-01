.PHONY: install test

install:
	uv cache clean --force kb-parser
	uv tool install --force .

test:
	uv run --extra test pytest test_parser.py -v
