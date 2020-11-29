# Turns2Network for MATSim-JLM

### Purpose, input/output
Class Turns2Network transforms original transport network 
and transit schedule according to the turn resctrictions.

Restrictions conflicted with the current transit lines 
are excluded from consideration.

Input data:
* nodes.csv - original network's nodes
* links.csv - original network's links
* line_path.csv - original transit lines
* EmmeManeuverRestrictions.csv - turns restrictions from EMME model 
* LinkCounts.csv - original traffic counts

Control parameters:
* `outPrefix` - prefix for output files (default: "t\_")
* `expRadius` - the expansion radius. If zero, all new nodes have the same coordinate and the new links with have length equals zero
* `offset`    - the offset between a link pair with the same incident nodes. If zero, the two new nodes created for that link pair will have the same coordinates

Output data are:
* `outPrefix`nodes.csv - transformed network's nodes
* `outPrefix`links.csv - transformed network's links
* `outPrefix`line_path.csv - transformed transit lines
* `outPrefix`LinkCounts.csv - transformed traffic counts
* nodes.geojson - original network's nodes in GeoJSON format (for debug)
* links.geojson - original network's links in GeoJSON format (for debug)
* `outPrefix`nodes.geojson - transformed network's nodes in GeoJSON format (for debug)
* `outPrefix`links.geojson - transformed network's links in GeoJSON format (for debug)

### Workflow
Transformation consists of the following steps:

1. Get all nodes - we will need their coordinates
2. Store BASE value for new keys
3. Get all links
4. Get all lines
5. Get all traffic counts data
6. Get turn restrictions' rules
7.  Export original network in GeoJSON format
8. For every node determine its in/out links and expand it to address the turn restrictions  
    8.1 Get in-links  
    8.2 Get out-links  
    8.3 Transform junction (`expand`) to address the turn restrictions  
    8.4 Change links  
    8.5 Change transit lines  
    8.6 Change counts readings  
9. Remove old nodes 
10. Export transformed network
11. Export to GeoJSON for debugging

### Nodes transformation

Algorithm mimics the one presented in 
[org.matsim.core.network.algorithms.NetworkExpandNode](https://www.matsim.org/apidocs/core/12.0/org/matsim/core/network/algorithms/NetworkExpandNode.html).
