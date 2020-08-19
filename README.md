# PCovidJatim - Worker

This is the worker of PCovidJatim. There are 3 kinds of workers: daily, fit_quick, and fit_test. Daily worker checks the data source for new data and save them if any. fit_quick worker re-fit the data without K-Fold. fit_test worker re-fit the data with K-Fold. The worker kind, K, and work days are defined as environment variables. There are other required environment variables too. Please check .env.

PCovidJatim is a webmap of forecast of COVID-19 spread in East Java. The data is obtained from the East Java government. It is then used to fit a SEICRD model with lmfit. The +30 days forecast of the model and the actual data are then mapped into a time aware webmap made with ArcGIS Online using its Python API. The webmap is loaded in an Arcgis Online web app and then loaded in the website in an iframe. In addition to the map, the website also offers the model scores and plots. Every day, a worker checks the data source for new data and update the forecast and map if any. Every few days, a worker re-fit the model and update the forecast and map. They're all hosted in heroku.

The site is available here: 
https://prediksicovidjatim.herokuapp.com

The paper is available here:
https://drive.google.com/file/d/1qbaiPBGEC2Iwqq2rxnZqlxS0gXQzYTXR/view?usp=sharing

The site source code is available here:
https://github.com/prediksicovidjatim/prediksicovidjatim

The core module is made as a pip installable package and is available here:
https://github.com/prediksicovidjatim/prediksicovidjatim-core

The web app is made with Arcgis Online Web AppBuilder Developer Edition. It is only a little bit modified to adjust the time slider. The source code is available here:
https://github.com/prediksicovidjatim/prediksicovidjatim-map

The worker source code is available here:
https://github.com/prediksicovidjatim/prediksicovidjatim-worker

Notebooks used in development is available here: 
https://github.com/prediksicovidjatim/prediksicovidjatim-notebooks

