## Utility Demand Pipeline
What started as a passion project is now an end to end pipeline that overlays weather and power grid data and loads it into a warehouse for downstream analytics. It is written in Python/PySpark and is platform agnostic (can run on any cloud or locally), portable (utilizes containers for rapid setup), and scalable (we can easily add more data sources and schemas without refactoring).

## Setup
This application has been containarized and all setup automated so there's not much legwork to be done.

First install [Docker Desktop](https://www.docker.com/products/docker-desktop/) and `just` via `brew install just`

Then clone the repository, open it, and run the following commands
```
just build
just run
```
And that's it! You can now perform testing within the container and manage versioning outside the container just as you normally would.

## Project Overview
TODO