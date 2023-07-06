# 🐙 The ODM Kraken
The **kraken** is the orchestration layer of the Ministry of Mobility's ODM data infrastructure, responsible for gathering and postprocessing mobility data.

## ODM Kraken is available as a conda package
from the `observatoire-mobilite` channel on [anaconda.org](https://anaconda.org/observatoire-mobilite)

```bash
conda install -c observatoire-mobilite odmkraken
```

## Start developping

The following instructions presume you have a working `conda` environment on your machine. Note, depending on your setup you may want to substitute `conda` for `mamba` or `micromamba`.

1. Clone this repo and `cd` to its root
2. Set up the development environment:
```bash
conda create -f conda/devel-env.yml
```
3. Activate the new environment:
```bash
conda activate odm-devel
```
4. Temporarily `cd` to the `src` directory
```bash
cd src
```
5. Run conda develop
```bash
conda develop .
```