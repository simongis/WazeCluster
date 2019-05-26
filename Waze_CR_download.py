from arcgis.gis import GIS
import arcgis.features

import time
import os
import sys
import zipfile

import arcpy

# Overwrite existing output
arcpy.env.overwriteOutput = True
arcpy.env.qualifiedFieldNames = False
arcpy.env.outputZFlag = "Disabled"


# globals
input_layer = "Alerts"  

def main():
    # Delete stuff
    cleanup()
    
    # connect to the porta where the Waze data isl
    gis = GIS("https://lucasl.maps.arcgis.com/","","")
    
    url = 'https://services9.arcgis.com/hbNUXYa0vWyWMkdB/arcgis/rest/services/Waze_Alerts_and_Traffic/FeatureServer'
    waze_flc = arcgis.features.FeatureLayerCollection(url, gis)
    
    # Check if sync enabled
    if waze_flc.properties.syncEnabled is True:
        print("Sync Enabled on " + waze_flc.url)
    else:
        print("Sync needs to be enabled on " + waze_flc.url)
        exit
        
    # Cant use Extract capability on a H-FS as it ends up using credits
	# https://community.esri.com/message/821828-re-extractdata-sample?commentID=821828#comment-821828
    replica = waze_flc.replicas.create(replica_name = 'Waze_Download',
                                       # Need to filter for the previous days worth of data?  Currently collecting 24hr of data
                                       # Could optionally apply a filter here for the type of incident we care about, such as JAMS
                                       layer_queries = {"0":{"queryOption": "useFilter", "where": "type = 'JAM'", "useGeometry": "false"}},
                                       layers = '0',
                                       data_format = 'filegdb',
                                       out_path = './download')
    
    print("fGDB downloaded " + replica)
    fgdb = unzip(replica)
    print (fgdb + " is the fGDB name") 
    
    # Strip Z Values
    remove_z(fgdb)
    
    # Run Density-based Clustering with the OPTICS Cluster Method 
    generate_density_based_clusters()
    
    # Generate ellipses on the cluster results
    generate_ellipses()
    
    # Calcaulte counts
    calc_counts()
    
    # Add a date field 
    calc_date()
    
    # Append results into Main Working GDB
    append_results()
    
    # Keep a rolling history, delete data from 14 days ago
    
    # Publish and overwrite Hosted Feature Service on ArcGIS Online
    publish_results()
    
def publish_results():
    # Sign in to portal
    arcpy.SignInToPortal("https://esriau.maps.arcgis.com/","","")
    
    # Set output file names
    outdir = os.path.join(os.path.dirname(__file__),  'sd')
    service = "FS_WazeEllipses_KL"
    sddraft_filename = service + ".sddraft"
    sddraft_output_filename = os.path.join(outdir, sddraft_filename)
    
    # Reference map to publish
    aprx = arcpy.mp.ArcGISProject(os.path.join(os.path.dirname(__file__),  'WazeForPublishing.aprx'))
    m = aprx.listMaps("Waze For Publishing")[0]
    
    # Create FeatureSharingDraft and set service properties
    sharing_draft = m.getWebLayerSharingDraft("HOSTING_SERVER", "FEATURE", service)
    sharing_draft.overwriteExistingService = "True"
    sharing_draft.portalFolder = "Waze"
    sharing_draft.summary = "Waze Ellipses created from a python script that pulls latest Waze data for Kuala Lumpur"
    sharing_draft.tags = "Waze, BGT, Kuala Lumpur"
    sharing_draft.description = "Latest Waze Jam incidents downloaded and a density clustering performed.  Ellipses generated around the main clusters"
    sharing_draft.credits = "Waze CCP"
    sharing_draft.useLimitations = "Demo Purposes Only"
    
    # Create Service Definition Draft file
    sharing_draft.exportToSDDraft(sddraft_output_filename)
    
    # Stage Service
    sd_filename = service + ".sd"
    sd_output_filename = os.path.join(outdir, sd_filename)
    arcpy.StageService_server(sddraft_output_filename, sd_output_filename)
    
    # Share to portal
    print("Uploading Service Definition for publishing")
    arcpy.UploadServiceDefinition_server(sd_output_filename, "My Hosted Services", in_override="OVERRIDE_DEFINITION", in_public="PUBLIC")
    
    print("Successfully Uploaded & Published.")

def append_results():
    print("Copying latest results into Main GDB")
    # arcpy.management.Append(r"D:\Work\waze_analysis\download\82c3be4239be45d68adad412a721432f.gdb\Waze_Ellipses", r"D:\Work\waze_analysis\wazeForPublish.gdb\WazeClusters", "TEST", None, None, "CLUSTER_ID > 0")
    arcpy.Append_management("Waze_Ellipses", r"..\..\wazeForPublish.gdb\WazeClusters", "TEST", None, None, "CLUSTER_ID > 0")
    
        
def calc_date():
    print("Creating a date field")
    arcpy.management.AddField("Waze_Ellipses", "ClusterDate", "DATE", None, None, None, None, "NULLABLE", "NON_REQUIRED", None)
    d = time.strftime("%d/%m/%Y")
    print("Updating date field with: " + str(d))  
    arcpy.management.CalculateField("Waze_Ellipses", "ClusterDate", 'time.strftime("%d/%m/%Y")', "PYTHON3", r"\n")
    

    
    
def calc_counts():
    #join Waze_OPTCS with Alerts
    # Join Waze_Optics with Waze Data
    cluster_original_join = arcpy.AddJoin_management("Waze_OPTICS", "SOURCE_ID", "Alerts", "ObjectId", "KEEP_ALL")
    # Count # waze incidents in each cluster
    summ_stats = arcpy.analysis.Statistics(cluster_original_join, "Alerts_Statistics", "Alerts.ObjectId COUNT", "Waze_OPTICS.CLUSTER_ID")
    arcpy.RemoveJoin_management (cluster_original_join)
    # Add Frequency Field
    arcpy.AddField_management("Waze_Ellipses", "TotalCount", "LONG", None, None, None, None, "NULLABLE", "NON_REQUIRED", None)
    # Join Ellipses to previously created Counts from above
    ellipses_counts_join = arcpy.AddJoin_management("Waze_Ellipses", "CLUSTER_ID", summ_stats, "CLUSTER_ID", "KEEP_ALL")
    # Update Counts Field
    # arcpy.CalculateField_management(ellipses_counts_join, "Waze_Ellipses.totalcounts", "!Static_Waze_Data_Statistics.FREQUENCY!", "PYTHON3", None)
    arcpy.management.CalculateField(ellipses_counts_join, "TotalCount", "!Alerts_Statistics.FREQUENCY!", "PYTHON3", None)
    arcpy.RemoveJoin_management (ellipses_counts_join)
    print("Counts added to Ellipses")
    

def generate_ellipses():
    try:
        # arcpy.management.MinimumBoundingGeometry("Waze_OPTICS", r"D:\Simon\Documents\ArcGIS\Projects\Waze_Python\Waze_Python.gdb\Waze_OPTICS_MinimumBoundingG", "CONVEX_HULL", "LIST", "CLUSTER_ID", "NO_MBG_FIELDS")
        arcpy.stats.DirectionalDistribution("Waze_OPTICS", "Waze_Ellipses", "1_STANDARD_DEVIATION", None, "CLUSTER_ID")
        print("Ellipses Layer created")
    except Exception:
        e = sys.exc_info()[1]
        print("Error creating Ellipses: " + e.args[0])
        exit
    
def generate_density_based_clusters():
    try:
        arcpy.stats.DensityBasedClustering("AlertsNoZ", "Waze_OPTICS", "OPTICS", 30, "2000 Meters", 35)
        print("Density Based CLustering Layer created")
    except Exception:
        e = sys.exc_info()[1]
        print("Error creating Cluster Layer: " + e.args[0])
        exit 

def remove_z(fgdb):
    ws = str(os.path.dirname(os.path.realpath(__file__))).replace("\\","/") + "/download/" + fgdb
    print("Workspace: " + ws)
    arcpy.env.workspace = ws
    # copy features and strip out Z values
    arcpy.CopyFeatures_management(input_layer, "AlertsNoZ")

def cleanup():
    path = './download'

    files = os.listdir(path)
    
    for f in files:
        f = os.path.join(path, f)
        # find any stray ZIPs older than one day and delete
        if time.time() - os.path.getmtime(f) > (60*60*24*1): # and f.endswith(".zip"):
            try:
                os.unlink(f)
                print ("Deleting old zip: " + f)
            except OSError:
                pass
        # Delete any GDB folders in this path
        if f.endswith(".gdb"):
            arcpy.Delete_management(f)
            print("Deleting GDB: " + f)    
    
def unzip(replica_zip):
   print("Unzipping: " + replica_zip)
   with zipfile.ZipFile(replica_zip,"r") as zip_ref:
    zip_ref.extractall('./download')
    
    for f in zip_ref.namelist():
        if f.endswith('gdb'):
            fgdb_name = f[:-4]
            
    zip_ref.close() 
    os.unlink(replica_zip)  # delete zip after unzipping
    return fgdb_name
    

# entry point
if __name__ == "__main__":
    main()
