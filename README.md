# NationalClearingCentre.ru
Методы с сайта NationalClearingCentre.ru (НКЦ, Национальный Клиринговый Центр)

### Метод
* Параметры ценных бумаг ([securInfo](https://www.nationalclearingcentre.ru/rates/securInfo))
* Параметры драгоценных металлов и иностранной валюты ([assetInfo](https://www.nationalclearingcentre.ru/rates/assetInfo))
* Расчетные цены на валютном рынке ([settlementPrices](https://www.nationalclearingcentre.ru/rates/settlementPrices)) - В разработке
* Календарь расчетных дней ([settleDates](https://www.nationalclearingcentre.ru/rates/settleDates))

### Необходимые пакеты Python
* ```bs4``` (получение DOM-дерева страницы, [подробнее..](https://pypi.org/project/beautifulsoup4/));
* ```lxml``` (парсер, который будет использован в связке с ```bs4```, [подробнее..](https://pypi.org/project/lxml/));
* ```fake_user_agent``` (генерация фейковых user-agent для запросов к серверу, [подробнее..](https://pypi.org/project/fake-useragent/));
* ```requests``` ([подробнее..](https://pypi.org/project/requests/));
* ```json```  (для работы с json: ```pip install json```);
* ```shutil``` (для работы с директориями: ```pip install shutil```);
* ```datetime``` (для работы с датой ```pip install datetime```).