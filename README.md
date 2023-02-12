# Batch processing in Spark using SBT tool and AWS EMR
Batch Spark job of CO2 Emission data using SBT tool.

According to Wikipedia : “Réseau de Transport d'Électricité ("Electricity Transmission Network"), usually known as RTE, is the electricity transmission system operator of France.”

source: https://en.wikipedia.org/wiki/R%C3%A9seau_de_Transport_d'%C3%89lectricit%C3%A9


According to https://www.rte-france.com/en/eco2mix/co2-emissions, the contribution of each energy source to C02 emissions is as follows:
- 0.986 t CO2 eq /MWh for coal-fired plants (Charbon)
- 0.777 t CO2 eq /MWh for oil-fired plants (Fioul)
- 0.429 t CO2 eq /MWh for gas-fired plants (Gaz)
- 0.494 t CO2 eq /MWh for biofuel plants (waste)


## Input
The electricity production data for 2020 published at https://eco2mix.rte-france.com/download/eco2mix/eCO2mix_RTE_Annuel-Definitif_2020.zip
