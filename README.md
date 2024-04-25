# Building and updating software datasets: an empirical assessment

This is the repository for the scripts developed for the study "Building and updating software datasets: an empirical assessment".

## Data collected
To retrieve the data generated for the study, there is a zip file retrievable in this [link](https://doi.org/10.5281/zenodo.11064396). Each folder inside the file 
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
For replication purposes, the datasets which contains recent projects from Github can be re-generated. To do so, the 
virtual environment must have installed the dependencies in "requirements.txt" file, add Github's tokens in "./token" 
file, download the source code scanner [Sourcemeter](https://sourcemeter.com/), re-define or leave as is the paths 
declared in the constants (variables written in caps) in the main method, and finally run "main.py" script. 
The script comprise 5 steps:
1. Project retrieval from Github: at first the sampling frame with projects complying with a specific quality criteria are retrieved from Github's API.
2. Create samples: with the sampling frame retrieved, the current samples are selected (currentSample and 
qualitasUpdated). In the case of qualitasUpdated, it is important to have first the "sample.csv" file inside the 
qualitas folder of the [dataset](https://doi.org/10.5281/zenodo.11064396) originally created for the study. This file contains the metadata of the projects in Qualitas Corpus.     
3. Project download and analysis: when all the samples are selected from the sampling frame (currentSample and 
qualitasUpdated), the repositories are downloaded and scanned with SourceMeter. In the cases in which the analysis is 
not possible, the projects are replaced with another one with similar size.
4. Outlier detection: once the datasets are collected, it is necessary to manually look for possible outliers in the code 
metrics under study. In the notebook "Experiment.ipynb" there are specific sections dedicated for it ("Outlier detection 
(Section 4.2.2)").
5. Outlier replacement: when the outliers are detected, in the same notebook there is also a section for outlier 
replacement ("Replace Outliers") where the outliers' url have to be listed to find the appropriate replacement.
* If it is required, the metrics from the Qualitas Corpus can also be re-generated. First, it is necessary to download 
the release 20130901r from its [official webpage](http://www.qualitascorpus.com/download/). Second, decompress the .tar files downloaded. Third, make sure 
that the compressed files with source code from the projects (.java files) are placed in the "compressed" folder, in 
some cases it is necessary to read the "QC_README" file in the project's folder. Finally, run the original main script 
"Generate metrics for the Qualitas Corpus (QC) dataset" part of the code.  
