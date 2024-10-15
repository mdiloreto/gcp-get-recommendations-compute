import json
from google.cloud import storage
from get_recommendations import GCPRecommender 
from datetime import datetime
from flask import abort

def recommendations_to_storage(request):
    if request.method != 'POST':
        return abort(405)  # Only allow POST method for this function.
    try:
        # Set up GCP Storage client
        try:
            print(f"Connecting Storage Client...")

            storage_client = storage.Client()
            bucket = storage_client.bucket('tw-gcp-recommendations-bucket-01')
            print(f"Connection sucesfully")            
        except Exception as e:
            print(f"Error connecting Storage Client: {e}", flush=True)
        try:
            # Fetch File Name with date time.
            print(f"Setting output file name")
            date_string = datetime.now().strftime("%Y%m%d")
            output_file_name = f'recommendations_{date_string}.csv'
        except Exception as e: 
            print(f"Error setting the file name: {e}", flush=True)            
        
        try:
            print(f"Inizializing recommender...")            
            recommender = GCPRecommender(output_file_name)
            print(f"Inizializing main function...")            
            recommender.main("468700285980") 
        except Exception as e:
            print(f"Error calling GCPRecommender Class: {e}", flush=True)

        try:
            print(f"Upload results to Bucket...")            
            
            # Upload the result to Cloud Storage
            blob = bucket.blob('VM-Recommendations/' + output_file_name)
            blob.upload_from_filename(output_file_name)
            
        except Exception as e:
            print(f"Error trying to upload the blob: {e}", flush=True)
    
    except Exception as e:
        print(f"Error uploading the file to Blob: {e}", flush=True) 

    return 'Recommendations processed and uploaded to Cloud Storage.'