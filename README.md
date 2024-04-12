## Fences

Given a labeled set of transaction records, train & validate a model to predict whether an incoming user is fraudulent, or not.

### Getting Started
Create a virtual environment and leverage a package installer like pip to install requirements.txt

#### Prerequisites
homebrew
python 3
pipx
poetry

#### Installing
```zsh
brew install pipx
pipx install poetry
```

Clone this github repo

From within the downloaded repo, run the following to load the data for exploration.

```zsh
poetry shell
python fences/load.py run
jupyter-notebook fences/transactions.ipynb
```

### Built With
* [Poetry](https://python-poetry.org) - Dependency Management & Packaging
* [Metaflow](https://metaflow.org) - Machine Learning Framework
