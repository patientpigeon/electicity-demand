default:
	just --list
build:
	docker build -t clearnorthern/electricity-demand-server:1 .
run:
	docker run -v $(pwd):/app -it --rm clearnorthern/electricity-demand-server:1
