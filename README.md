# Building and updating software datasets: an empirical assessment

This is the repository for the scripts developed for the study "Building and updating software datasets: an empirical assessment".

## Data collected
To retrieve the data generated for the study, there is a zip file retrievable in this [link](https://doi.org/10.5281/zenodo.11059969). Each folder inside the file 
corresponds to one of the datasets of projects employed in the study (qualitas, currentSample and qualitasUpdated). 
Every dataset comprised three files "class.csv", "method.csv" and "sample.csv", with class metrics, method metrics and
repository metadata of the projects respectively. Here is a description of the datasets:
+ qualitas: includes code metrics and repository metrics from the projects in the release 20130901r of the Qualitas Corpus.
+ currentSample: includes code metrics and repository metrics from a recent sample collected with our sampling procedure.
+ qualitasUpdated: includes code metrics and repository metrics from an updated version of the Qualitas Corpus applying our maintenance procedure.

## Plot graphics
To plot the results and graphics in the article there is a Jupyter Notebook "Experiment.ipynb".
It is initially configured to use the data in "datasets" folder.

## Replication Kit
For replication purposes, new data can be generated. To do so, the virtual environment must have installed the 
dependencies in "requirements.txt" file, add Github's tokens in ./token, download the source code scanner [Sourcemeter](https://sourcemeter.com/), 
define or leave as is the paths declared in the constants (variables declared in caps), and finally run "main.py" script. 
The script comprise 5 steps:
+ Project retrieval: at first the quality projects are retrieved from Github's API.
+ Create samples: with the sampling frame retrieved, the current samples are selected (currentSample and qualitasUpdated).
+ Project download and analysis: when all the samples are selected from the sampling frame (qualitas, currentSample and
qualitasUpdated), the repositories are downloaded and scanned with the tool SourceMeter. In the cases in which the 
analysis is not possible, the projects are replaced with another one with similar size.
+ Outlier detection: once the datasets are retrieved, it is necessary to look for possible outliers in the code metrics 
under study. In the notebook "Experiment.ipynb" there are specific sections dedicated for it ("Outlier detection 
(Section 4.2.2)").
+ Outlier replacement: when the outliers are detected, in the same notebook there is also a section for outlier 
replacement ("Replace Outliers")