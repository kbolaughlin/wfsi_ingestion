import requests
import json
import urllib.parse
import xml.etree.ElementTree as E
from ckanapi import RemoteCKAN
import datetime
import os
from dotenv import load_dotenv

load_dotenv()

apiKey = os.environ['apiKey']
ckan = RemoteCKAN('https://wifire-data.sdsc.edu/', apikey=apiKey)

# ckan.action.package_delete(id='test_pyrolysis_gases_measured_by_ftir_spectroscopy_in_a_wind_tunnel_and_at_ft_jackson_sc')
# ckan.action.package_delete(id='test_pyrolysis_gases_produced_by_fast_and_slow_pyrolysis_of_foliage_samples_from_15_plants_common_to')
# ckan.action.package_delete(id='test_field_measurements_of_coarse_wood_duff_litter_and_trees_for_the_eglin_air_force_base_2023_fuels')
# ckan.action.package_delete(id='test_pyrolysis_gases_measured_by_gas_chromatography_and_mass_spectrometry_from_fires_in_a_wind_tunne')

url = 'https://wfsi-data.org/catalog/d1/mn/v2/query/solr/?q=%20-obsoletedBy:*%20AND%20%20-formatId:*dataone.org%5C%2Fcollections*%20AND%20%20-formatId:*dataone.org%5C%2Fportals*%20AND%20formatType:METADATA&fl=id,seriesId,title,origin,pubDate,dateUploaded,abstract,resourceMap,beginDate,endDate,read_count_i,geohash_9,datasource,isPublic,documents,sem_annotation,northBoundCoord,southBoundCoord,eastBoundCoord,westBoundCoord&sort=dateUploaded+desc&rows=25&start=0&facet=true&facet.sort=index&facet.field=geohash_2&facet.mincount=1&facet.limit=-1&wt=json'
response = requests.get(url).json()
results = response['response']
results = [x for x in results['docs'] if x['isPublic']]


for res in results:
    
    # fetch metadata file
    encoded = urllib.parse.quote(f'''(id="{res['id']}" OR seriesId="{res['id']}")''') 
    url = 'https://wfsi-data.org/catalog/d1/mn/v2/query/solr/?q=' + encoded + '&fl=abstract,id,seriesId,fileName,resourceMap,formatType,formatId,obsoletedBy,isDocumentedBy,documents,title,origin,keywords,attributeName,pubDate,eastBoundCoord,westBoundCoord,northBoundCoord,southBoundCoord,beginDate,endDate,dateUploaded,archived,datasource,replicaMN,isAuthorized,isPublic,size,read_count_i,isService,serviceTitle,serviceEndpoint,serviceOutput,serviceDescription,serviceType,project,dateModified&wt=json&rows=1000&archived=archived:*'
    response = requests.get(url).json()
    resources = response['response']['docs']
    metadata = [x for x in resources if 'formatType' in x and x['formatType'] == 'METADATA' and x['id'] == res['id']][0]

    doi_id = urllib.parse.quote(metadata['id'])

    # fetch EML file
    eml_url = 'https://wfsi-data.org/catalog/d1/mn/v2/object/' + doi_id

    print(eml_url)    
    response = requests.get(eml_url)
    tree = E.fromstring(response.content)

    # parse all necessary attributes
    dataset = tree.find('dataset')
    dataset_title = dataset.find('title').text
    notes = dataset.find('abstract').find('para').text
    tags = [{"name": "".join([ c if c.isalnum() else "" for c in j.text ]) } for sub in dataset.findall('keywordSet') for j in sub.findall('keyword')]
    license = dataset.find('intellectualRights').find('para').text

    # get bounding box information
    coverages = dataset.find('coverage').findall('geographicCoverage')
    coordinates = []

    westBoundingCoordinates = []
    eastBoundingCoordinates = []
    northBoundingCoordinates = []
    southBoundingCoordinates = []

    for bbox_coverage in coverages:
        coverage = bbox_coverage.find('boundingCoordinates')
        westBoundingCoordinates.append(float(coverage.find('westBoundingCoordinate').text))
        eastBoundingCoordinates.append(float(coverage.find('eastBoundingCoordinate').text))
        northBoundingCoordinates.append(float(coverage.find('northBoundingCoordinate').text))
        southBoundingCoordinates.append(float(coverage.find('southBoundingCoordinate').text))

    westBoundingCoordinate = min(westBoundingCoordinates)
    eastBoundingCoordinate = max(eastBoundingCoordinates)
    northBoundingCoordinate = max(northBoundingCoordinates)
    southBoundingCoordinate = min(southBoundingCoordinates)

        
    bbox = {"type": "Polygon", 
        "coordinates":  [[[eastBoundingCoordinate, southBoundingCoordinate  ],
                            [ westBoundingCoordinate, southBoundingCoordinate ],
                            [ westBoundingCoordinate, northBoundingCoordinate  ],
                            [ eastBoundingCoordinate, northBoundingCoordinate ],
                            [ eastBoundingCoordinate, southBoundingCoordinate ]
                        ]]}
    
    # temporal range
    rangeOfDates = dataset.find('coverage').find('temporalCoverage').find('rangeOfDates')
    beginTime = rangeOfDates.find('beginDate').find('calendarDate').text
    endDate = rangeOfDates.find('endDate').find('calendarDate').text
    package_temporal = {"endTime": endDate, "startTime": beginTime}

    # point of contact information 
    poc = dataset.find('contact')

    maintainer = []

    if poc.find('individualName').find('givenName') is not None \
        and poc.find('individualName').find('surName') is not None:

        maintainer.append(poc.find('individualName').find('givenName').text + ' ' + poc.find('individualName').find('surName').text)
        
    if poc.find('electronicMailAddress') is not None:
       maintainer.append(poc.find('electronicMailAddress').text)

    if poc.find('orcid') is not None:
        maintainer.append(poc.find('userId').text)


    # extract methods
    methods = dataset.find('methods').find('methodStep').find('description').findall('para')
    method_description = (' ').join([x.text for x in methods])
        

    # list creater information
    creaters = dataset.findall('creator')
    creator_text = []

    for creater in creaters:
        info = []
        info.append(creater.find('individualName').find('givenName').text + ' ' + creater.find('individualName').find('surName').text)
        if creater.find('electronicMailAddress') is not None:
            info.append(creater.find('electronicMailAddress').text)

        if creater.find('organizationName') is not None:
            info.append(creater.find('organizationName').text)

        if creater.find('userId') is not None:
            info.append(creater.find('userId').text)

        info_text = ", ".join(info)
        creator_text.append(info_text)

    # list associatedParty information
    associated_party = dataset.findall('associatedParty')
    associated_party_text = []

    for ap in associated_party:
        info = []
        info.append(ap.find('individualName').find('givenName').text + ' ' + ap.find('individualName').find('surName').text)
        if ap.find('electronicMailAddress') is not None:
            info.append(ap.find('electronicMailAddress').text)

        if ap.find('organizationName') is not None:
            info.append(ap.find('organizationName').text)

        if ap.find('userId') is not None:
            info.append(ap.find('userId').text)

        if ap.find('role') is not None:
            info.append(ap.find('role').text)

        info_text = ", ".join(info)
        associated_party_text.append(info_text)    


    extra_fields = [
        {
            'value' : json.dumps(bbox),
            'key' : 'spatial'
        },
        {
            'value' : json.dumps(package_temporal),
            'key' : 'temporal'
        },
        {
            "value" : method_description,
            "key" : 'method'
        },
        {
            'value': res['id'],
            'key' : 'doi'
        }
    ]
    
    if len(creator_text) > 0:
        extra_fields.append({
            "value" :' | '.join(creator_text),
            "key" : "creators"
        })

    if len(maintainer) > 0:
        extra_fields.append({
            "value" : ", ".join(maintainer),
            "key" : "maintainor"
        })

    if len(associated_party_text) > 0:
        extra_fields.append({
            "value" : " | ".join(associated_party_text),
            "key" : "associated_parties"
        })    

    project = dataset.find('project')

    if project is not None:
        extra_fields.append({
            "value" : project.find('title').text,
            "key" : "project"
        })

        award = project.find('award')
        if award is not None:
            extra_fields.append({
                "value" : award.find('title').text,
                "key" : "award"
            })
             
            award_number = award.find('award_number')
            if award_number is not None:
                extra_fields.append({
                    "value" : award_number.text,
                    "key" : "award_number"
                })

            funder_name = award.find('funderName')
            if funder_name is not None:
                funder = funder_name.text

                funderIdentifier = award.find('funderIdentifier')
                if funderIdentifier is not None:
                    funder = funder + ', ' + funderIdentifier.text 

                extra_fields.append({
                    "value" : funder,
                    "key" : "funder"
                })

    extra_fields.append({
        "value" : 'https://wfsi-data.org/view/' + doi_id,
        "key" : 'Source'
    })

    # parse name for validity 
    name = None
    name = dataset_title

    if ',' in name:
        name = name.replace(',', "")
    
    if len(name) > 100:
        name = name[:100]

    name = name.lower()
    name = name.replace(' ', '_')
    name = name.replace('.', '')
    print(name)

    intellectualRights = dataset.find('intellectualRights')  
    license_id = None
    license_title = None
    license_url = None

    if intellectualRights is not None:            
        license_info = intellectualRights.find('para').text
        if 'Creative Commons Universal 1.0' in license_info:
            license_id = 'CC0-1.0'
            
        if 'Creative Commons Attribution 4.0' in license_info:
            license_id = 'CC-BY-4.0'


    # extract resources
    resources = dataset.findall('otherEntity')

    resource_json = []

    for resource in resources:
        resource_name = resource.find('entityName').text
        resource_type = resource.find('entityType').text
        resource_url = "https://wfsi-data.org/catalog/d1/mn/v2/object/" + resource.attrib['id']

        format = None
        if resource_name.endswith('.csv'):
            format = "CSV"
        if resource_name.endswith('.zip'):
            format = 'ZIP'
        if resource_name.endswith('.xml'):
            format = 'XML'
        if resource_name.endswith('.txt'):
            format = 'TXT'

        resource_info = {
            "resource_name" : resource_name,
            "resource_type" : resource_type,
            "resource_url" : resource_url,
            "format" : format,
        }

        if license is not None:
            resource_info['license_id'] = license


        resource_json.append(resource_info)


    # delete previous version
    ckan.action.package_delete(id=name)

    # save package
    try: 
        request = ckan.action.package_create(name=name, 
                                        title = dataset_title,                                        
                                        owner_org = 'wfsi',
                                        extras=extra_fields,
                                        notes = notes,
                                        tags=tags,
                                        license_id=license_id
                                    )
    except Exception as e:
        print(name)
        print(e)

    package_id = request["id"]

    print("package created: " + dataset_title + ', ' + package_id)

    # if package successfully saves, save resources to ckan
    if package_id != None:
        for resource in resource_json:
            response = requests.post('https://wifire-data.sdsc.edu/api/action/resource_create',
                    data={"package_id" : package_id,
                            'format' : resource['format'],
                            'url' : resource['resource_url'],
                            "name" : resource['resource_name'],
                            "mime_type" : resource['resource_type'],
                            "created" : datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S.%f%z'),
                            "last_modified" : datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S.%f%z'),
                            },
                    headers={"X-CKAN-API-Key": apiKey})
