import json
import csv
from google.cloud import recommender_v1
from google.protobuf.json_format import MessageToDict
from google.cloud import resourcemanager_v3
from google.protobuf import json_format
from get_all_projects import GetGCPFoldersAndProjects



class GCPRecommender:
    def __init__(self, output_file_name):
        self.output_file_name = output_file_name
        self.recommender_types= ["google.compute.instance.MachineTypeRecommender", 
                                 "google.compute.instance.IdleResourceRecommender"]
        self.region_to_zones_map = {
            "na": [
                "us-central1-a", "us-central1-b", "us-central1-c", "us-central1-f",
                "us-east1-b", "us-east1-c", "us-east1-d",
                "us-east4-a", "us-east4-b", "us-east4-c",
                "us-west1-a", "us-west1-b", "us-west1-c",
                "northamerica-northeast1-a", "northamerica-northeast1-b", "northamerica-northeast1-c",
                "northamerica-northeast2-a", "northamerica-northeast2-b", "northamerica-northeast2-c",
                "southamerica-east1-a", "southamerica-east1-b", "southamerica-east1-c",
                "southamerica-west1-a", "southamerica-west1-b", "southamerica-west1-c",
                "us-east5-a", "us-east5-b", "us-east5-c",
                "us-south1-a", "us-south1-b", "us-south1-c",
                "us-west2-a", "us-west2-b", "us-west2-c",
                "us-west3-a", "us-west3-b", "us-west3-c",
                "us-west4-a", "us-west4-b", "us-west4-c"
            ],

            "apac":[
                "asia-east1-a", "asia-east1-b", "asia-east1-c",
                "asia-southeast1-a", "asia-southeast1-b", "asia-southeast1-c",
                "asia-northeast1-a", "asia-northeast1-b", "asia-northeast1-c",
                "asia-south1-a", "asia-south1-b", "asia-south1-c",
                "asia-east2-a", "asia-east2-b", "asia-east2-c",
                "asia-northeast2-a", "asia-northeast2-b", "asia-northeast2-c",
                "asia-southeast2-a", "asia-southeast2-b", "asia-southeast2-c",
                "australia-southeast1-a", "australia-southeast1-b", "australia-southeast1-c",
                "australia-southeast2-a", "australia-southeast2-b", "australia-southeast2-c"
            ],
            "amr": [
                "us-central1-a", "us-central1-b", "us-central1-c", "us-central1-f",
                "us-east1-b", "us-east1-c", "us-east1-d",
                "us-east4-a", "us-east4-b", "us-east4-c",
                "us-west1-a", "us-west1-b", "us-west1-c",
                "northamerica-northeast1-a", "northamerica-northeast1-b", "northamerica-northeast1-c",
                "northamerica-northeast2-a", "northamerica-northeast2-b", "northamerica-northeast2-c",
                "southamerica-east1-a", "southamerica-east1-b", "southamerica-east1-c",
                "southamerica-west1-a", "southamerica-west1-b", "southamerica-west1-c",
                "us-east5-a", "us-east5-b", "us-east5-c",
                "us-south1-a", "us-south1-b", "us-south1-c",
                "us-west2-a", "us-west2-b", "us-west2-c",
                "us-west3-a", "us-west3-b", "us-west3-c",
                "us-west4-a", "us-west4-b", "us-west4-c"
            ],
            "emea": [
                "europe-west1-b", "europe-west1-c", "europe-west1-d",
                "europe-west2-a", "europe-west2-b", "europe-west2-c",
                "europe-west3-a", "europe-west3-b", "europe-west3-c",
                "europe-west4-a", "europe-west4-b", "europe-west4-c",
                "europe-central2-a", "europe-central2-b", "europe-central2-c",
                "europe-north1-a", "europe-north1-b", "europe-north1-c",
                "europe-southwest1-a", "europe-southwest1-b", "europe-southwest1-c",
                "europe-west6-a", "europe-west6-b", "europe-west6-c",
                "europe-west8-a", "europe-west8-b", "europe-west8-c",
                "europe-west9-a", "europe-west9-b", "europe-west9-c",
                "europe-west10-a", "europe-west10-b", "europe-west10-c",
                "europe-west12-a", "europe-west12-b", "europe-west12-c",
                "me-central1-a", "me-central1-b", "me-central1-c",
                "me-central2-a", "me-central2-b", "me-central2-c",
                "me-west1-a", "me-west1-b", "me-west1-c"
            ]

        }
        
    def get_zones_by_project_prefix(self, project_name):
        for prefix, zones in self.region_to_zones_map.items():
            if project_name.startswith(prefix):
                return zones
        print(f"No zone mapping found for project: {project_name}", flush=True)
        return None

    def convert_and_append_json_to_csv(self, recommendations_json, zone, project_id):
        print(f"Converting JSON to CSV for zone: {zone}", flush=True)
        csv_objects = []
        recommendation = json.loads(recommendations_json)
        try:
            recommendation_id = recommendation.get('name')
            primary_impact_category = recommendation.get('primaryImpact', {}).get('category')
            cost_projection_units = recommendation.get('primaryImpact', {}).get('costProjection', {}).get('cost', {}).get('units')
            cost_projection_nanos = recommendation.get('primaryImpact', {}).get('costProjection', {}).get('cost', {}).get('nanos')
            recommendation_state = recommendation.get('stateInfo', {}).get('state')
            last_refresh_time = recommendation.get('lastRefreshTime')
            recommender_subtype = recommendation.get('recommenderSubtype')
            description = recommendation.get('description')
            
            # Assuming 'content' contains 'overview' which has details about the current and recommended machine types
            content = recommendation.get('content', {}).get('overview', {})
            resource_name = content.get('resourceName', "N/A")
            recommended_action = content.get('recommendedAction', "N/A")
            current_machine_type = content.get('currentMachineType', {}).get('name', "N/A")
            recommended_machine_type = content.get('recommendedMachineType', {}).get('name', "N/A")

            csv_object = {
                "ProjectID": project_id,
                "Zone": zone,
                "ResourceName": resource_name,
                "RecommenderSubtype": recommender_subtype,
                "RecommendedAction": recommended_action,
                "PrimaryImpactCategory": primary_impact_category,
                "Description": description,
                "CostProjectionUnits": cost_projection_units,
                "CostProjectionNanos": cost_projection_nanos,
                "RecommendationState": recommendation_state,
                "LastRefreshTime": last_refresh_time,
                "CurrentMachineType": current_machine_type,
                "RecommendedMachineType": recommended_machine_type,
                "RecommendationID": recommendation_id,
            }
            csv_objects.append(csv_object)
            print(f"Conversion successful for zone: {zone}", flush=True)
        except Exception as e:
            print(f"Error converting JSON to CSV for zone {zone}: {e}", flush=True)
        
        return csv_objects
        
    def list_recommendations(self, project_id, zone, recommender_id):
        recommendations_list = []
        client = recommender_v1.RecommenderClient()
        try:
            parent = f"projects/{project_id}/locations/{zone}/recommenders/{recommender_id}"
            recommendations = client.list_recommendations(request={"parent": parent})
            for recommendation in recommendations:
                recommendation_json = json_format.MessageToJson(recommendation._pb)
                recommendations_list.append(recommendation_json)
        except Exception as e:
            print(f"An error occurred while listing recommendations for {project_id} in {zone}: {e}")
        return recommendations_list


    def main(self, organization_id):
        csv_data = []
        try:
            if organization_id == "0":
                get_projects_class = GetGCPFoldersAndProjects(organization_id)
                get_projects = get_projects_class.get_all_projects_no_org()
            else:          
                get_projects_class = GetGCPFoldersAndProjects(organization_id)
                get_projects = get_projects_class.get_all_projects_with_org()

            print(get_projects)
        except Exception as e:
            print(f"An error occurred during processing: {e}")   
             
        try:
            for project_id in get_projects:
                print(f"Processing recommendations for Project {project_id}", flush=True)
                zones = self.get_zones_by_project_prefix(project_id)
                if zones:
                    for zone in zones:
                        print(f"Processing recommendations for zone: {zone}", flush=True)
                        for recommender_type in self.recommender_types:
                            recommendations = self.list_recommendations(project_id, zone, recommender_type)
                            if recommendations:  # Assuming convert_and_append_json_to_csv returns a list
                                for recommendation in recommendations:
                                    # print("Object type:", type(recommendation))
                                    # print(recommendation)
                                    recommendations_output = self.convert_and_append_json_to_csv(recommendation, zone, project_id)
                                    print(recommendations_output)
                                    if recommendations_output:
                                        csv_data.extend(recommendations_output)
        except Exception as e:
            print(f"An error occurred during processing: {e}")
            
            
        ##### EXPORT CSV TO FILE <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
        
        try:
            if csv_data:  # Check if there's anything to write
                with open(self.output_file_name, mode='w', newline='', encoding='utf-8') as file:
                    writer = csv.DictWriter(file, fieldnames=csv_data[0].keys())
                    writer.writeheader()
                    for data in csv_data:
                        writer.writerow(data)
                print("Data exported successfully to", self.output_file_name, flush=True)
        except Exception as e:
            print(f"Failed to write data to {self.output_file_name}: {e}")
