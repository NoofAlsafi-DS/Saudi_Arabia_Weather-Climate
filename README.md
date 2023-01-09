# Saudi Arabia’s Weather and Climate — Vision 2030
 <img width="955" alt="Screen Shot 2023-01-09 at 13 54 15" src="https://user-images.githubusercontent.com/117467806/211292154-e517a155-0829-4a82-a782-b02223ed8f64.png">
 
### Introduction
Weather data are interesting resources, not only for weather forecasting but also for dozens of other purposes across different industries. A central area is certainly understanding climate change. A very large amount of weather data is recorded because of the number of sites, the number of data elements and because the observations are recorded hourly.
Data scientists analyze weathr data collected from weather stations around the globe to forecast how climate trends and events unfold. Because the amounts of data, climate analytics is a common application area for Big Data technology and distributed computing.

### Weather and Climate in 2030 Vision
Saudi Arabia vision 2030 cuts back conventional reliance on oil, that can potentially move Saudi Arabia toward creating a less oil-centered, more environmentally-focused economy. The Kingdom also aims to lower annual carbon emissions up to 130 million tons by 2030 and reach net-zero by 2060 by investing in clean energy projects, including solar energy masses.

### Problem Statement
Weather and climate affect people around the world. Rising global temperatures are expected to raise sea levels, and change precipitation and other local climate conditions.

### Goal
The goal in our project is to predict the air Temperature by using Machine Learning with PySpark. We will analyze the data and create a model for the prediction. We aim to explore the relations between climate variables such as air temperate, wind, Sky condition, Visibility and Dew.

### Dataset
We will train our model with Saudi Arabia hourly climate integrated surface data with the below data observations:
1. Wind
2. Sky condition
3. Visibility
4. Air temperature
5. Dew
6. Sea level pressure
The dataset contains 1852380 rows and 35 columns. it contains data from the year 2017 till 2022.

### Exploratory Data Analysis
#### 1st Plot: Air Temperature Categories Distribution:
<img width="820" alt="Screen Shot 2023-01-09 at 14 04 51" src="https://user-images.githubusercontent.com/117467806/211294791-1b71908c-e97f-4caf-83fb-7611a03d7d46.png">

#### 2nd Plot: Year Distribution:
<img width="756" alt="Screen Shot 2023-01-09 at 14 05 07" src="https://user-images.githubusercontent.com/117467806/211299418-6393ec27-81e2-4db0-91f7-8eb1161c85cf.png">

#### 3rd Plot: Station Country Distribution:
<img width="778" alt="Screen Shot 2023-01-09 at 14 05 31" src="https://user-images.githubusercontent.com/117467806/211299546-8a0fa5d8-8ed4-4956-9927-18e4cb603674.png">

#### 4th Plot: Humidity Levels Distribution:
<img width="798" alt="Screen Shot 2023-01-09 at 14 05 43" src="https://user-images.githubusercontent.com/117467806/211299776-d36d499d-ca26-430d-a360-a04767ebc86f.png">


#### 5th Plot: Cloud And Visibility OK Distribution:
<img width="761" alt="Screen Shot 2023-01-09 at 14 06 01" src="https://user-images.githubusercontent.com/117467806/211299865-0435901f-fa74-42d0-b1ef-7a3b719c5e0b.png">

#### 6th Plot: Total Observations in each regions in 2021:
<img width="788" alt="Screen Shot 2023-01-09 at 14 06 16" src="https://user-images.githubusercontent.com/117467806/211299980-5f9faad7-b327-4e9e-9809-a906715c8757.png">

#### 7th Plot: Air Temperature Variations over Years:
<img width="662" alt="Screen Shot 2023-01-09 at 14 06 34" src="https://user-images.githubusercontent.com/117467806/211300107-db37e05b-a7f0-46e3-9c0e-9cabc3aeadac.png">



#### 8th Plot: Wind Speed Variations over Years:
<img width="716" alt="Screen Shot 2023-01-09 at 14 06 42" src="https://user-images.githubusercontent.com/117467806/211300226-08f78396-2310-44ea-9caa-4cbd56e1aac6.png">

#### 9th Plot: Air Temperature Variations over Month:
<img width="751" alt="Screen Shot 2023-01-09 at 14 06 54" src="https://user-images.githubusercontent.com/117467806/211300371-5b19a18f-8239-4df0-a1cf-134b2e41292b.png">


#### 10th Plot: Air Temperature Categories Per Year:
<img width="750" alt="Screen Shot 2023-01-09 at 14 07 04" src="https://user-images.githubusercontent.com/117467806/211300495-1faae61a-455d-47a5-9250-3c8299f27a04.png">

#### 11th Plot: Wind Speed Rate According To Air Temperature Categories:
<img width="768" alt="Screen Shot 2023-01-09 at 14 07 14" src="https://user-images.githubusercontent.com/117467806/211300628-843cab68-1f22-4cf1-a81e-64b291d8e9b6.png">

### Dashboards
After cleaning and processed the dataset we used Tableau to create dashboards to find insights within the dataset. We created Two dashboards where each of them is sharing a coherent story.

#### 1st Dashboard:
##### In the first Dashboard the story describe the relationship between the weather and Tourism Sectors in Saudi Arabia:
Saudi Arabia is widely seen as the final frontier of tourism. As the birthplace of Islam, and a millennia-old crossroads of pilgrims and traders, the Kingdom’s rich culture and diverse heritage has no equal. In September 2019, Saudi Arabia opened its doors to the world for the first time. The launch of the Kingdom’s tourism visa saw visitors from around the globe flock to explore Saudi’s ancient history, striking landscapes, and warm hospitality. Last year, Saudi Arabia welcomed 67 million visitors, a sure sign that Vision 2030 is shaping the future of the Kingdom’s travel and tourism sector.
##### Goal:
Climate and weather are important factors in tourists’ decision making and also influence the successful operation of tourism businesses. With a wide range of terrains — from mountains to deserts to beaches — it’s not surprising that Saud’s weather is also diverse. And while certain generalities apply, there are important differences between regions to consider when planning a visit to Saudi Arabia. So here from the climate and weather data we have will provide an overview of the ideal time to visit any city in Saudi Arabia.
![b352fa88-d542-40b2-a3ca-fc636e936e12](https://user-images.githubusercontent.com/117467806/211301255-e31492f2-94fb-4b3a-9bd7-211eae3d5c23.JPG)

##### The Dashboard outcomes:
With an array of climates, cultural events and adventure, there is no wrong time to visit Saudi. The question is, when is the best time for you to visit Saudi? So through the optimum weather or the reason for visit saudi it can decide which city is best for visit in any season according to the insights we provided. For example:
1. In Summer, Abha and Taif are have a pleasant atmosphere and Multiple adventures to do such as hiking and camping
2. Jeddah has a warm atmosphere in winter and also you can do a surfing and diving in these season.
3. To do Umrah the best season for visit Makkah is in fall and winter where the weather in these two season is warmer with average temperature between 29 -32 °C.

#### 2nd Dashboard:
##### In the second Dashboard the story describe the relationship between the weather and Renewable energy Sectors in Saudi Arabia:
The Kingdom of Saudi Arabia has a distinct geographical and climatic location that makes utilizing renewable energy sources economically attractive, supporting Saudi efforts to diversify the domestic energy mix. This interest is rapidly increasing in line with Saudi Arabia’s Vision 2030 that aims at achieving sustainable economic, social, and environmental developments.
##### Goal:
Renewable energy technologies are highly dependent on climate-related factors including sunlight, wind speed and water availability. So here from the climate and weather data we have will provide an overview of the optimum situation in different city in Saudi Arabia to apply the Renewable energy in it.
![192be274-a214-4159-b730-13cb2685ea00](https://user-images.githubusercontent.com/117467806/211301628-3f1a84fe-114d-4048-ba7e-91db936d053a.JPG)

##### The Dashboard outcomes:
By 2030, the contribution of renewable energy to the overall energy mix will reach up to 50%. Renewable energy projects are one of the key drivers towards achieving sustainability that will contribute to avoiding emissions and the displacement of high-value fuel in electricity generation. The weather dataset of Saudi Arabia that we have its a great for forcast potential of Renewable Energy Sector in Saudi Arabia. The outcomes:
1. The optimum city for wind speed that have best weather circumstances according to available data we have are Taif , Yenbo , and Wejh which have average temperature in each quarter of the year between 35–40 °C and wind speed around 10–15 m/s.
2. According to provided data the result shows the cities that have optimum temperature with perfect humidity levels in all year quarters are Abha, Guriat, and Khamis Mushait.

Pig Latin Exploratory Data Analysis
We used Pig Latin to answer 20 interesting questions about our dataset that will help as find directions for our project and shape our investigation.











[Saudi Arabia’s_Weather&Climate _Vision_2030_Medium.pdf](https://github.com/NoofAlsafi-DS/weathers/files/10372335/Saudi.Arabia.s_Weather.Climate._Vision_2030_Medium.pdf)
