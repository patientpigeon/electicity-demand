## Utility Demand Pipeline
What started as a passion project is now an end-to-end pipeline that overlays weather and power grid data and loads it into a warehouse for downstream analytics. It's written in Python/PySpark and is platform agnostic (can run on any cloud or locally), portable (utilizes containers for rapid setup), and scalable (we can easily add more data sources and schemas without refactoring).

## Setup
This application has been containarized and all local setup automated so there's not much legwork to be done.

- First install [Docker Desktop](https://www.docker.com/products/docker-desktop/) and `just` via `brew install just`

- Then clone this repository, open it, and run the command `just run`

And that's it! You can now perform testing within the container and manage versioning on your host machine just as you normally would.

_Note that the Dockerfile used to build the image has been left in this repository so you use it to build your own image or understand requirements._

## Project Overview
TODO
