import redis
import uuid
import json

print("Welcome")


#r = redis.Redis(host='localhost', port=6389)

json_string_value = '''{ 
"idIndividu": "01010101K",
"codeAssedic": "000",
"idNationaleInterne": "8888888888",
"statutIndemnisable": "UUUUUUUUUU",
"topActualisable": false,
"topTeleDec": true,
"aEteActualise": true,
"rci": "1000036888",
"statutIntermittent": "NON",
"antenneIndividu": "039",
"civilite": "MME",
"nom": "MARTIN",
"prenom": "MICHELLE",
"nomCorrespondance": "MARTIN",
"prenomCorrespondance": "MICHELLE",
"codePostal": "10300",
"dateDeNaissance": "1969-03-12",
"tel1": "010101",
"dossierReferentDE": "RE",
"pecSousType": "1",
"pecType": "1",
"codeCategorie": "C1",
"topCreateurEntreprise": true,
"indicateurRadiation": "I",
"codeSafir": "787878",
"cleMiseAJourIdBatch": "XXXXXXXXXXXXXXXX",
"idLignePourLeBatch": 535
}'''

json_value = json.loads(json_string_value)


#So we can load this using multiple loaders
def update_records(start,to):
    # Taille du pipeline
    pipeline_size = 500
    pipe = r.pipeline(transaction=False)
    batch = 1
    print(f"starting update ")
    for i in range(start,to):
        json_value["idIndividu"] = str(i)
        json_value["cleMiseAJourIdBatch"] =  str(uuid.uuid4())


        # r.pipeline().set(f"json:{i}", json.dumps(data))
        pipe.json().set("DemandeurEmploiTampon:"+str(i), '$', json_value)

        if (i + 1) % pipeline_size == 0:
            pipe.execute()
            #time.sleep(1)
            print(f"batch {str(batch)} ")
            batch=batch+1

    #the end batch
    pipe.execute()
    print(f"end update ")



def delete_records(start,to):
    # Taille du pipeline
    pipeline_size = 500
    pipe = r.pipeline(transaction=False)
    batch = 1
    print(f"starting delete ")
    for i in range(start,to):
        pipe.json().delete("DemandeurEmploiTampon:"+str(i))

        if (i + 1) % pipeline_size == 0:
            pipe.execute()
            #time.sleep(1)
            print(f"batch {str(batch)} ")
            batch=batch+1

    #the end batch
    pipe.execute()
    print(f"end update ")


if __name__ == '__main__':
    # adjust as required
    update_records(0, 2400000)
    #delete_records(2400000,2434903)

