# spark-python-example

Pyspark Example for stackoverflow question



## For Linux environment

```
wget https://repo.continuum.io/miniconda/Miniconda3-latest-Linux-x86_64.sh

```

## For Mac environment [Conda](https://conda.io/docs/user-guide/tasks/manage-environments.html)

```
brew cask install Caskroom/cask/miniconda
conda env create -f environment.yml --name pyspark
conda create --name pyspark   python=3 numpy scipy tzlocal python-dateutil slackclient
source activate pyspark
conda info —envs
conda env export | cut -f 1 -d '=' > environment.yml

```

## how to pack

```
zip -r pyspark_env.zip /usr/local/miniconda3/envs/pyspark

```

## setup alias

Add Following alias in your .bashrc or .zshrc
 
```
alias pe=". /usr/local/miniconda3/bin/activate"
```

Then execute 

```
pe pyspark
```