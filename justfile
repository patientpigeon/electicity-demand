# list available recipes
default:
	just --list

# build an image using the local Dockerfile
build:
	docker build -t utility-demand .

# run a container based on ptarmagain's image
run:
	docker run -v $(pwd):/app -it --rm ptarmagain/utility-demand
