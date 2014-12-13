# DRAFT 

## On the client and server

Add a 'rome' sign to the signpost, because all roads lead to Rome. 

`sudo vi /var/www/rome.json` 

    { 
      "server": "buya",
      "port": "6379" 
    }


`sudo vi /etc/hosts`

        127.0.0.1   localhost signpost

`sudo pip install redis`


## Test 

    wget -qO - http://signpost/rome.json 
    { 
      "server": "buya",
      "port": "6379" 
    }


