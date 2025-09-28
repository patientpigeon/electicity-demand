default:
	just --list
setup:
	docker build -t clearnorthern/electricity-demand-server .
	docker run -v $(pwd):/app -it --rm clearnorthern/electricity-demand-server:1

run_container:
	docker run -v $(pwd):/app -it --rm clearnorthern/electricity-demand-server:1
