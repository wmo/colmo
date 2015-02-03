# DRAFT 

## On the client and server

Add a 'rome' sign to the localhost, because all roads lead to Rome. 

`sudo vi /var/www/rome.json` 

    { 
      "redisserver": "<NAME_OF_YOUR_REDIS_SERVER>",
      "redisport": "6379" 
    }


`sudo pip install redis`


## Test 

    wget -qO - http://localhost/rome.json 
    { 
      "redisserver": "<NAME_OF_YOUR_REDIS_SERVER>",
      "redisport": "6379" 
    }


## Server only 

Install git and python: 

    sudo apt-get install git python python-pip octave 

Note: no need to install octave if you are not going to run octave jobs. 

You'll also need apache, just for the localhost directing you to Rome. So if you don't have it running yet: 

    sudo apt-get install apache2

Edit rome.json. Depending on how your apache is configured you may need to put it in /var/www or /var/www/html: 

    sudo vi /var/www/html/rome.json

or 

    sudo vi /var/www/rome.json

and put these lines:

    { 
      "redisserver": "<NAME_OF_YOUR_REDIS_SERVER>",
      "redisport": "6379" 
    }

SIGNPOST edit!!

redisclient:
`sudo pip install redis`



Clone the scripts:
    cd 
    git clone https://github.com/wmo/colmo 

 
    export PATH=$HOME/colmo:$PATH 



