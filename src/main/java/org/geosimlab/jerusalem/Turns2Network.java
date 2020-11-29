package org.geosimlab.jerusalem;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.NumberFormatException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.CSVWriter;

import org.apache.log4j.Logger;
import org.apache.log4j.BasicConfigurator;

public class Turns2Network {

	private static final Logger log = Logger.getLogger(Turns2Network.class);
	private final String turnRestrictionsFilename;
	private final String nodesFilename;
	private final String linksFilename;
	private final String linepathsFilename;
	private final String countsFilename;
	private final String outputPrefix;
	
	private Integer ID_BASE = null;
	private double expRadius = 1.0;
	private double offset = 0.0;
	private double dist = 1.0;

	/**
	 * @param nodesFilename path to the network's nodes file.
	 * Format description see in {@linkplain #parseNodes()} 
	 * 
	 * @param linksFilename path to the network's links file.
	 * Format description see in {@linkplain #parseLinks()}
	 *  
	 * @param linepathsFilename path to the transit lines file.
	 * Format description see in {@linkplain #parseLines()}
	 * 
	 * @param turnRestrictionsFilename path to the turns restrictions file.
	 * Format description see in {@linkplain #parseTurnRestrictions()}
	 * 
	 * @param countsFilename path to the traffic counts file.
	 * Format description see in {@linkplain #parseTrafficCounts()}
	 * 
	 * @param outPrefix Prefix for output files
	 * 
	 * @param expRadius the expansion radius. If zero, all new nodes have the same coordinate
	 * and the new links with have length equals zero
	 * 
	 * @param offset the offset between a link pair with the same incident nodes. If zero, the two new
	 * nodes created for that link pair will have the same coordinates
	 */
	public Turns2Network(String nodesFilename, String linksFilename, 
			     String linepathsFilename, String turnRestrictionsFilename,
			     String countsFilename, String outPrefix, 
			     final double expRadius, final double offset) {
		
		BasicConfigurator.configure();
		
		this.nodesFilename = nodesFilename;
		this.linksFilename = linksFilename;
		this.linepathsFilename = linepathsFilename;
		this.turnRestrictionsFilename = turnRestrictionsFilename;
		this.countsFilename = countsFilename;
		this.outputPrefix = outPrefix;
		
		this.setExpRadius(expRadius);
		this.setOffset(offset);
	}
	
	public Integer createId() {
		this.ID_BASE = this.ID_BASE + 1;
		return this.ID_BASE - 1; 
	}

	public void setExpRadius(final double expRadius) {
		if (Double.isNaN(expRadius)) {
			throw new IllegalArgumentException("Expansion radius must not be NaN.");
		}
		this.expRadius = expRadius;
		this.dist = Math.sqrt(this.expRadius * this.expRadius - this.offset * this.offset);
	}

	public void setOffset(final double offset) {
		if (Double.isNaN(offset)) {
			throw new IllegalArgumentException("Expansion offset must not be NaN.");
		}
		this.offset = offset;
		this.dist = Math.sqrt(this.expRadius * this.expRadius - this.offset * this.offset);
	}
	
	public void buildNetwork() {
		// Get all nodes - we will need their coordinates
		Map<Integer, Node> nodesMap = this.parseNodes();
		log.info("Parsed network's nodes");
		// Store BASE value for new keys
		this.ID_BASE = nodesMap.keySet().stream().max(Integer::compare).get() + 1;
		// Get all links
		Map<String, Link> linksMap = this.parseLinks();
		log.info("Parsed network's links");
		// Get all lines
		Map<String, Line> linesMap = this.parseLines();
		log.info("Parsed transit lines file");
		// Get all traffic counts data
		ArrayList<String[]> countsList = this.parseTrafficCounts();
		log.info("Parsed traffic counts");
		// Get turn restrictions' rules
		Map<Integer, ArrayList<Integer[]>> restrictions = this.parseTurnRestrictions(linesMap);
		log.info("Parsed turn restrictions");
		
		// Export original network in GeoJSON format
		this.exportNodesGeoJSON(nodesMap, false);
		this.exportLinksGeoJSON(linksMap, nodesMap, false);
		
		// Store links to be removed
		ArrayList<String> oldLinks = new ArrayList<String>();
		// Store new version of links with traffic counters
		Map<Integer, ArrayList<Integer[]>> trafficCountersMap = new HashMap<Integer, ArrayList<Integer[]>>();
		
		// For every node determine its in/out links and expand it to address the turn restrictions
		for(Integer nodeId : restrictions.keySet()) {
			// Place for new links
			Map<String, Link> newLinks = new HashMap<String, Link>();
			
			// Get in-links
			Map<String, List<Object>> inLinks = linksMap.entrySet().stream()
				.filter(x -> x.getValue().toNode.equals(nodeId))
				.collect(Collectors.toMap(
					Map.Entry::getKey,
					x -> List.of(x.getValue(), nodesMap.get(x.getValue().fromNode))
				));
			oldLinks.addAll(inLinks.keySet());
			// Get out-links
			Map<String, List<Object>> outLinks = linksMap.entrySet().stream()
				.filter(x -> x.getValue().fromNode.equals(nodeId))
				.collect(Collectors.toMap(
					Map.Entry::getKey,
					x -> List.of(x.getValue(), nodesMap.get(x.getValue().toNode))
				));
			oldLinks.addAll(outLinks.keySet());
			
			// Transform junction to address the turn restrictions
			List<Object> expanded = this.expandNode(nodesMap.get(nodeId), inLinks, outLinks, restrictions.get(nodeId));
			nodesMap.putAll((Map<Integer, Node>) expanded.get(0));

			// Change links.csv
			Map<String, Link> newInLinks = ((Map<Integer, Link>) expanded.get(1)).entrySet().stream()
				.collect(Collectors.toMap(
					e -> e.getValue().getId(),
                    e -> e.getValue())
				);
			newLinks.putAll(newInLinks);
			
			Map<String, Link> newOutLinks = ((Map<Integer, Link>) expanded.get(2)).entrySet().stream()
				.collect(Collectors.toMap(
					e -> e.getValue().getId(),
                    e -> e.getValue())
				);
			newLinks.putAll(newOutLinks);
			
			Map<String, Link> newInterLinks = ((Map<Integer[], Link>) expanded.get(3)).entrySet().stream()
				.collect(Collectors.toMap(
					e -> e.getValue().getId(),
                    e -> e.getValue())
				);
			newLinks.putAll(newInterLinks);
			
			linksMap.putAll(newLinks);
			
			// Remove old nodes and links
			for (String key : oldLinks) {
				linksMap.remove(key);
			}
			
			// Change transit lines
			for(String lineId : linesMap.keySet()) {
				linesMap.get(lineId).expandLineAtNode(
					nodeId, 
					(Map<Integer, Link>) expanded.get(1),
					(Map<Integer, Link>) expanded.get(2),
					(Map<Integer[], Link>) expanded.get(3)
				);
			}
			
			// Change counts readings
			Map<Integer, Integer> neighborInIds = newInLinks.values().stream()
				.collect(Collectors.toMap(s -> s.fromNode, s -> s.toNode));
			
			Map<Integer, Integer> neighborOutIds = newOutLinks.values().stream()
				.collect(Collectors.toMap(s -> s.toNode, s -> s.fromNode));
			
			Set<Integer> neighbors = neighborInIds.keySet().stream()
				.collect(Collectors.toSet());
			
			neighbors.addAll(neighborOutIds.keySet().stream()
				.collect(Collectors.toSet()));		
			
			// newDLinks[old_node_i] = [neighbor_j, in_node_i, out_node_i]
			ArrayList<Integer[]> newDLinks = new ArrayList<Integer[]>();
			for(Integer neighborId : neighbors) {
				newDLinks.add(new Integer[] {
					neighborId,
					neighborInIds.get(neighborId),
					neighborOutIds.get(neighborId)
				});
			}	
			trafficCountersMap.put(nodeId, newDLinks);
		}
		
		// Remove old nodes 
		for(Integer nodeId : restrictions.keySet()) {		
			Long cnt = linksMap.entrySet().stream()
				.filter(x -> x.getValue().toNode.equals(nodeId) || x.getValue().fromNode.equals(nodeId))
				.count();	
			if (cnt == 0)  
				nodesMap.remove(nodeId);
		}
		
		// Export transformed network
		this.exportNodes(nodesMap);
		this.exportLinks(linksMap);
		this.exportLines(linesMap);
		this.exportTrafficCounts(countsList, trafficCountersMap);
		log.info("Exported transformed network and transit lines");
		
		// Export to GeoJSON for debugging
		this.exportNodesGeoJSON(nodesMap, true);
		this.exportLinksGeoJSON(linksMap, nodesMap, true);
		log.info("Exported transformed network in GeoJSON format");
	}

	/**
	 * Expands the specified {@link Node node} and adds turning restrictions. 
	 * 
	 * <p>Algorithm mimics the one presented in 
	 * <a href="https://www.matsim.org/apidocs/core/12.0/org/matsim/core/network/algorithms/NetworkExpandNode.html">
	 * org.matsim.core.network.algorithms.NetworkExpandNode</a>
	 * </p>
	 * 
	 * <p>It is done in the following way:
	 * <ol>
	 * <li>Creates for each in- and out-link a new node with
	 * <ul>
	 * <li><code>new_nodeId = nodeId+"-"+index; index=[0..#incidentLinks]</code></li>
	 * <li><code>new_coord</code> with distance <code>r</code> to the given node
	 * in direction of the corresponding incident Link of the given node with
	 * offset <code>e</code>.</li>
	 * </ul>
	 * <pre>
	 * {@code
	 * <-----12------         <----21-------
	 *
	 *            x-0 o     o 1-5
	 *                   O nodeId = x
	 *            x-1 o     o x-4
	 *             x-2 o   o x-3
	 * ------11----->         -----22------>
	 *               |       ^
	 *               |       |
	 *              32       31
	 *               |       |
	 *               |       |
	 *               v       |
	 * }
	 * </pre>
	 * </li>
	 * <li>Connects each incident link of the given node with the corresponding <code>new_node</code>
	 * <pre>
	 * {@code
	 * <-----12------ o     o <----21-------
	 *                   O
	 * ------11-----> o     o -----22------>
	 *                 o   o
	 *                 |   ^
	 *                 |   |
	 *                32   31
	 *                 |   |
	 *                 |   |
	 *                 v   |
	 * }
	 * </pre>
	 * </li>
	 * <li>Removes the given node from the network
	 * <pre>
	 * {@code
	 * <-----12------ o     o <----21-------
	 *
	 * ------11-----> o     o -----22------>
	 *                 o   o
	 *                 |   ^
	 *                 |   |
	 *                32   31
	 *                 |   |
	 *                 |   |
	 *                 v   |
	 * }
	 * </pre>
	 * </li>
	 * <li>Inter-connects the <code>new_node</code>s with new links as defined in the
	 * <code>prohibitedTurns</code> list, with:<br>
	 * <ul>
	 * <li><code>new_linkId = fromLinkId+"-"+index; index=[0..#turn-tuples]</code></li>
	 * <li>length equals the Euclidean distance</li>
	 * <li>new link's attributes are equals to the attributes of the fromLink.</li>
	 * </ul>
	 * <pre>
	 * {@code
	 * <-----12------ o <--21-0-- o <----21-------
	 *
	 * ------11-----> o --11-1--> o -----22------>
	 *                 \         ^
	 *              11-2\       /31-3
	 *                   \     /
	 *                    v   /
	 *                    o   o
	 *                    |   ^
	 *                    |   |
	 *                   32   31
	 *                    |   |
	 *                    |   |
	 *                    v   |
	 * }
	 * </pre>
	 * </li>
	 * </ol>
	 * </p>
	 *
	 * @param node the {@link Node} to expand
	 * @param inLinks the Map of in-links for the given <code>node</code>
	 * @param outLinks the Map of out-links for the given <code>node</code>
	 * @param prohibitedTurns the list of turns prohibited at the given <code>node</code>
	 * @return The List of replacements for the given <code>node</code>, all its in/out links, and virtual interlinks
	 */
	public List<Object> expandNode(Node node, 
			Map<String, List<Object>> inLinks, Map<String, List<Object>> outLinks,
			ArrayList<Integer[]> prohibitedTurns) {
		
		Map<Integer, Node> expandedNodes = new HashMap<Integer, Node>();
		Map<Integer, Link> expandedInLinks = new HashMap<Integer, Link>();
		Map<Integer, Link> expandedOutLinks = new HashMap<Integer, Link>();
		Map<Integer[], Link> expandedInterLinks = new HashMap<Integer[], Link>();
		
		Integer idx = 0;
		double d = this.dist;
		double eps = this.offset;
		for (String linkId : inLinks.keySet()) {
			Node inNode = (Node) inLinks.get(linkId).get(1);
			
			Double diffX = inNode.getCoordX() - node.getCoordX(); 
			Double diffY = inNode.getCoordY() - node.getCoordY();
			
			double l = Math.sqrt(diffX*diffX + diffY*diffY);
			if (Math.abs(l) < 1e-8) {
				l = d;
			}
			double dx = diffX / l;
			double dy = diffY / l;
			double x = node.getCoordX() + d * dx - eps * dy;
			double y = node.getCoordY() + d * dy + eps * dx;			
			
			Node newNode = new Node(this.createId(), new Double[] {x, y});
			expandedNodes.put(newNode.getId(), newNode);
			
			Link inLink = (Link)inLinks.get(linkId).get(0);
			Link newLink = new Link(inLink.getLinkRow());
			newLink.toNode = newNode.getId();
			newLink.length = Double.toString(l);
			expandedInLinks.put(newLink.fromNode, newLink);
			
			idx = idx + 1;
		}
		
		for (String linkId : outLinks.keySet()) {
			Node outNode = (Node) outLinks.get(linkId).get(1);
			
			Double diffX = outNode.getCoordX() - node.getCoordX(); 
			Double diffY = outNode.getCoordY() - node.getCoordY();
			
			double l = Math.sqrt(diffX*diffX + diffY*diffY);
			if (Math.abs(l) < 1e-8) {
				l = d;
			}
			double dx = diffX / l;
			double dy = diffY / l;
			double x = node.getCoordX() + d * dx + eps * dy;
			double y = node.getCoordY() + d * dy - eps * dx;			
			
			Node newNode = new Node(this.createId(), new Double[] {x, y});
			expandedNodes.put(newNode.getId(), newNode);

			Link outLink = (Link)outLinks.get(linkId).get(0);
			Link newLink = new Link(outLink.getLinkRow());
			newLink.fromNode = newNode.getId();
			newLink.length = Double.toString(l);
			expandedOutLinks.put(newLink.toNode, newLink);
			
			idx = idx + 1;
		}
				
		for (Integer inLinkId : expandedInLinks.keySet()) 
			for (Integer outLinkId: expandedOutLinks.keySet()) {
				// Check if there is exists rule for the turn inLinkId->outLinkId
				Long cnt = prohibitedTurns.stream()
					.filter(s -> s[0].equals(inLinkId) && s[1].equals(outLinkId))
					.count();
				// Connect expanded nodes by virtual interlinks
				if (cnt == 0) {
					Link inLink = expandedInLinks.get(inLinkId);
					Link interLink = new Link(inLink.getLinkRow());
					interLink.fromNode = inLink.getToNode();
					interLink.toNode = expandedOutLinks.get(outLinkId).getFromNode();
					
					Node from = expandedNodes.get(interLink.fromNode);
					Node to = expandedNodes.get(interLink.toNode);
					Double diffX = to.getCoordX() - from.getCoordX(); 
					Double diffY = to.getCoordY() - from.getCoordY();
					double l = Math.sqrt(diffX*diffX + diffY*diffY);
					if (Math.abs(l) < 1e-8) {
						l = d;
					}
					interLink.length = Double.toString(l);
					Integer[] interKey = {interLink.fromNode, interLink.toNode};
					expandedInterLinks.put(interKey, interLink);
				}
			}
       	
		return List.of(expandedNodes, expandedInLinks, expandedOutLinks, expandedInterLinks);
	}

	/**
	 * Format of nodes CSV:
	 * <table border="1">
	 * <tr>
	 *     <td>i</td> <td>is_centroid</td> <td>x</td>  <td>y</td>
	 * </tr>
	 * <tr>
	 *     <td>1</td> <td>True</td> <td>220839.8958</td> <td>632216.2776999999</td>
	 * </tr>
	 * <tr>
	 *     <td>2</td> <td>True</td> <td>220916.36049999998</td> <td>632004.1671</td>
	 * </tr>
	 * </table>
	 * 
	 * @return the Map with nodes IDs as keys and Node objects as values
	 */
	public Map<Integer, Node> parseNodes() {
		Map<Integer, Node> nodesMap = new HashMap<Integer, Node>();
        CSVReader csvReader = null;
		try {
			csvReader = new CSVReaderBuilder(new FileReader(this.nodesFilename)).withSkipLines(1).build();
			String[] row;
			Integer cnt = 1;
			while ((row = csvReader.readNext()) != null) {
				Node node = new Node(Integer.valueOf(row[0]), new Double[] {
					Double.valueOf(row[2]), 
					Double.valueOf(row[3])
				});
				nodesMap.put(Integer.valueOf(row[0]), node);
				cnt++;
			}
			csvReader.close();
		} catch (IOException e) {
			log.error("ERROR: Cannot read turn restrictions file: " + this.nodesFilename);
		} catch (NumberFormatException e) {
			log.error("ERROR: Check format of turn restrictions file: " + this.nodesFilename);
		}
		
		return nodesMap;
	}

	public void exportNodes(Map<Integer, Node> nodesMap) {
        CSVWriter csvWriter = null;
		File f = new File(this.nodesFilename);
		String filename = "";
		if (f.getParent() != null)
			filename = f.getParent() + "/";
		filename = filename + this.outputPrefix + f.getName();
		try {
			csvWriter = new CSVWriter(new FileWriter(filename));
			csvWriter.writeNext(new String[]{"i", "is_centroid", "x", "y"}, false);
			for(Integer nodeId : nodesMap.keySet()) {
				Node node = nodesMap.get(nodeId);
				String[] csvRow = {node.getId().toString(), "True", 
					node.getCoordX().toString(), node.getCoordY().toString()};
				csvWriter.writeNext(csvRow, false);				
			}
			csvWriter.close();
		} catch (IOException e) {
			log.error("ERROR: Cannot write nodes file: " + filename);
		} catch (NumberFormatException e) {
			log.error("ERROR: Check format of nodes file: " + filename);
		}
	}
	
	
	public void exportNodesGeoJSON(Map<Integer, Node> nodesMap, boolean withTurns) {
		
		BufferedWriter writer = null;		
		File f = new File(this.nodesFilename);
		String filename = "";
		if (f.getParent() != null)
			filename = f.getParent() + "/";
		if (withTurns)
			filename = filename + "t_";
		filename = filename + "node.geojson";
		
		try {
			writer = new BufferedWriter(new FileWriter(filename));
			String row = "{\"type\":\"FeatureCollection\",\n";
			row = row + "\"crs\":{\"type\": \"name\",\"properties\":{\"name\": \"EPSG:2039\"}},\n";
			row = row + "\"features\":[";
			writer.write(row);
			boolean isFirst = true;
			for(Map.Entry<Integer, Node> entry : nodesMap.entrySet()) {
				if (isFirst) {
					row = "\n{\"type\":\"Feature\", ";
					isFirst = false;
				} else {
					row = ",\n{\"type\":\"Feature\", ";
				}
				row = row + "\"properties\":{\"id\":\"" + entry.getValue().getId() + "\"},";
				row = row + "\"geometry\":{\"type\":\"Point\",";
				row = row + "\"coordinates\":[" + entry.getValue().getCoordX() + "," + entry.getValue().getCoordY() + "]}}";
				writer.write(row);				
			}
			writer.write("\n]}");
			writer.close();
		} catch (IOException e) {
			log.error("ERROR: Cannot export nodes in GeoJSON format: " + filename);
		}
	}
	
	/**
	 * Format of links CSV:
	 * <table border="1">
	 * <tr>
	 *     <td>i</td> <td>j</td> <td>length_met</td> 
	 *     <td>mode</td> <td>num_lanes</td>  <td>num_lanes</td>
	 *     <td>@at</td> <td>@linkcap</td>  <td>s0link_m_per_s</td>
	 * </tr>
	 * <tr>
	 *     <td>1</td> <td>10215</td> <td>70.29802322</td> 
	 *     <td>"[Mode(c), Mode(w)]"</td> <td>9.0</td>  <td>9</td>
	 *     <td>0.0</td> <td>0.0</td>  <td>0.0</td>
	 * </tr>
	 * </table>
	 * 
	 * @return the Map with links IDs as keys and Link objects as values
	 */
	public Map<String, Link> parseLinks() {
		Map<String, Link> linksMap = new HashMap<String, Link>();
        CSVReader csvReader = null;
		try {
			csvReader = new CSVReaderBuilder(new FileReader(this.linksFilename)).withSkipLines(1).build();
			String[] row;
			while ((row = csvReader.readNext()) != null) {
				String id = row[0] + "_" + row[1] + "_" + row[5];
				linksMap.put(id, new Link(row));
			}
			csvReader.close();
		} catch (IOException e) {
			log.error("ERROR: Cannot read file with links: " + this.linksFilename);
		} catch (NumberFormatException e) {
			log.error("ERROR: Check format of file with links: " + this.linksFilename);
		}
		
		return linksMap;
	}
	
	public void exportLinks(Map<String, Link> linksMap) {
        CSVWriter csvWriter = null;
		File f = new File(this.linksFilename);
		String filename = "";
		if (f.getParent() != null)
			filename = f.getParent() + "/";
		filename = filename + this.outputPrefix + f.getName();
		try {
			csvWriter = new CSVWriter(new FileWriter(filename));
			csvWriter.writeNext(new String[]{
				"i", "j", "length_met",  "mode",
				"num_lanes", "type", "@at", "@linkcap", "s0link_m_per_s"}, false);
			for(String linkId : linksMap.keySet()) {
				Link link = linksMap.get(linkId);
				csvWriter.writeNext(link.getLinkRow(), false);				
			}
			csvWriter.close();
		} catch (IOException e) {
			log.error("ERROR: Cannot write links file: " + filename);
		} catch (NumberFormatException e) {
			log.error("ERROR: Check format of links file: " + filename);
		}
	}
	
	public void exportLinksGeoJSON(Map<String, Link> linksMap, Map<Integer, Node> nodesMap, boolean withTurns) {
		File f = new File(this.linksFilename);
		String filename = "";
		if (f.getParent() != null)
			filename = f.getParent() + "/";
		if (withTurns)
			filename = filename + "t_";
		filename = filename + "links.geojson";
		BufferedWriter writer = null;		
		try {
			writer = new BufferedWriter(new FileWriter(filename));
			String row = "{\"type\":\"FeatureCollection\",\n";
			row = row + "\"crs\":{\"type\": \"name\",\"properties\":{\"name\": \"EPSG:2039\"}},\n";
			row = row + "\"features\":[";
			writer.write(row);
			boolean isFirst = true;
			for(Map.Entry<String, Link> entry : linksMap.entrySet()) {
				Node fromNode = nodesMap.get(entry.getValue().getFromNode());
				Node toNode = nodesMap.get(entry.getValue().getToNode());
				String coords = "[" + fromNode.getCoordX() + "," + fromNode.getCoordY() + "],";
				coords = coords + "[" + toNode.getCoordX() + "," + toNode.getCoordY() + "]";
				if (isFirst) {
					row = "\n{\"type\":\"Feature\", ";
					isFirst = false;
				} else {
					row = ",\n{\"type\":\"Feature\", ";
				}
				row = row + "\"properties\":{\"id\":\"" + entry.getValue().getId() + "\"},";
				row = row + "\"geometry\":{\"type\":\"LineString\",";
				row = row + "\"coordinates\":[" + coords + "]" + "}}";
				writer.write(row);
			}
			writer.write("\n]}");
			writer.close();
		} catch (IOException e) {
			log.error("ERROR: Cannot export links in GeoJSON format: " + filename);
		}
	}
	
	/**
	 * Format of line_path CSV:
	 * <table border="1">
	 * <tr>
	 *     <td>line</td> <td>i</td> <td>j</td> <td>length_met</td> <td>number</td> <td>is_stop</td>
	 * </tr>
	 * <tr>
	 *     <td>ALRT11</td> <td>51028</td> <td>51027</td> <td>772.8809118270874</td> <td>0</td> <td>1</td>
	 * </tr>
	 * <tr>
	 *     <td>ALRT11</td> <td>51027</td> <td>51026</td> <td>524.1227149963379</td> <td>1</td> <td>1</td>
	 * </tr>
	 * </table>
	 * 
	 * @return the Map with line IDs as keys and Line objects as values
	 */
	public Map<String, Line> parseLines() {
		Map<String, Line> linesMap = new HashMap<String, Line>();
        CSVReader csvReader = null;
		try {
			csvReader = new CSVReaderBuilder(new FileReader(this.linepathsFilename)).withSkipLines(1).build();
			String[] row;
			while ((row = csvReader.readNext()) != null) {
				String id = row[0];
				Integer seqNumber = Integer.parseInt(row[4]);
				LineSegment segment = new LineSegment(
					Integer.valueOf(row[1]), 
					Integer.valueOf(row[2]), 
					row[3], row[5]);
				if (linesMap.get(id) == null) {
					linesMap.put(id, new Line(id));
					linesMap.get(id).addSegment(segment);
				} else {
					linesMap.get(id).addSegment(segment, seqNumber);
				}			
			}
			csvReader.close();
		} catch (IOException e) {
			log.error("ERROR: Cannot read lines file: " + this.linepathsFilename);
		} catch (NumberFormatException e) {
			log.error("ERROR: Check format of lines file: " + this.linepathsFilename);
		}
		return linesMap;
	}
	
	public void exportLines(Map<String, Line> linesMap) {
        CSVWriter csvWriter = null;
		File f = new File(this.linepathsFilename);
		String filename = "";
		if (f.getParent() != null)
			filename = f.getParent() + "/";
		filename = filename + this.outputPrefix + f.getName();
		try {
			csvWriter = new CSVWriter(new FileWriter(filename));
			csvWriter.writeNext(new String[]{"line", "i", "j", "length_met", "number", "is_stop"}, false);
			for(String lineId : linesMap.keySet()) {
				Line line = linesMap.get(lineId);
				for (int i = 0; i < line.getSegments().size(); i++) {
					String[] seg = line.getSegments().get(i).getSegmentRow();
					ArrayList<String> csvRow = new ArrayList<String>();
					csvRow.add(lineId);
					csvRow.add(seg[0]);
					csvRow.add(seg[1]);
					csvRow.add(seg[2]);
					csvRow.add(Integer.toString(i));
					csvRow.add(seg[3]);
					csvWriter.writeNext(csvRow.toArray(new String[csvRow.size()]), false);	
				}
			}
			csvWriter.close();
		} catch (IOException e) {
			log.error("ERROR: Cannot write lines file: " + filename);
		} catch (NumberFormatException e) {
			log.error("ERROR: Check format of lines file: " + filename);
		}
	}
	
	/**
	 * Format of restrictions CSV:
	 * <table border="1">
	 * <tr>
	 *     <td>c</td> <td>At</td> <td>From</td>  <td>To</td> <td>TPF</td>
	 * </tr>
	 * <tr>
	 *     <td>a</td> <td>10001</td> <td>10002</td> <td>14254</td> <td>-1</td>
	 * </tr>
	 * <tr>
	 *     <td>a</td> <td>10001</td> <td>10003</td> <td>15876</td> <td>0</td>
	 * </tr>
	 * </table>
	 * TPF = -1 means "no restrictions", TPF = 1 means "turn From--To at node At is prohibited".
	 * 
	 * @return the Map with nodes IDs as keys and lists of prohibited turns at this node as values
	 */
	public Map<Integer, ArrayList<Integer[]>> parseTurnRestrictions(Map<String, Line> linesMap) {
		Map<Integer, ArrayList<Integer[]>> restrictions = new HashMap<Integer, ArrayList<Integer[]>>();
        CSVReader csvReader = null;
		try {
			csvReader = new CSVReaderBuilder(new FileReader(this.turnRestrictionsFilename)).withSkipLines(1).build();
			String[] row;
			Map<Integer, ArrayList<Integer[]>> restrictionsMap = new HashMap<Integer, ArrayList<Integer[]>>();
			ArrayList<Integer> badRestrictions = new ArrayList<Integer>();
			while ((row = csvReader.readNext()) != null) {
				final Integer at = Integer.valueOf(row[1]);
				final Integer from = Integer.valueOf(row[2]);
				final Integer to = Integer.valueOf(row[3]);
				// Extract prohibited turns only, U-turns are excluded too
				if ((Integer.valueOf(row[4]) == 0) && !from.equals(to)) {
					Integer[] turnEnds = { from, to };
					// Check the conflicts with transit lines 
					Long cnt = linesMap.values().stream()
						.filter(s -> s.containsTurn(at, from, to))
						.count();
					// Keep restriction only if there is no conflicts with the transit lines
					if (cnt != 0) {
						badRestrictions.add(at);
					}
					if (restrictionsMap.get(at) == null) {
						restrictionsMap.put(at, new ArrayList<Integer[]>() {
							{ add(turnEnds); }
						});
					} else {
						restrictionsMap.get(at).add(turnEnds);
					}					
				}
			}
			restrictions = restrictionsMap.entrySet().stream()
				.filter(s -> !badRestrictions.contains(s.getKey()))
				.collect(Collectors.toMap(s -> s.getKey(), s -> s.getValue()));
			
			csvReader.close();
		} catch (IOException e) {
			log.error("ERROR: Cannot read turn restrictions file: " + this.turnRestrictionsFilename);
		} catch (NumberFormatException e) {
			log.error("ERROR: Check format of turn restrictions file: " + this.turnRestrictionsFilename);
		}
		
		return restrictions;
	}

	/**
	 * Format of LinkCounts CSV:
	 * 
	 * LinkID, CID, A, B, COUNTDATE, factor, 
	 * AB0600, BA0600, AB0700, BA0700, AB0800, BA0800,
	 * AB0900, BA0900, AB1000, BA1000, AB1100, BA1100,
	 * AB1200, BA1200, AB1300, BA1300, AB1400, BA1400, 
	 * AB1500, BA1500, AB1600, BA1600, AB1700, BA1700, 
	 * AB1800, BA1800
     * 
     * 4573, 1296, 75010, 75015, 09/09/2015, 1, 
     * 74,99,116,227,118,146,101,101,59,83,89,86,79,74,90,75,128,100,157,109,180,132,157,156,148,128
	 * 
	 * @return the ArrayList of counts readings 
	 */
	public ArrayList<String[]> parseTrafficCounts() {
		ArrayList<String[]> countsList = new ArrayList<String[]>();
        CSVReader csvReader = null;
		try {
			csvReader = new CSVReaderBuilder(new FileReader(this.countsFilename)).withSkipLines(1).build();
			String[] row;
			while ((row = csvReader.readNext()) != null) {
				//Integer[] countsKey = {Integer.valueOf(row[2]), Integer.valueOf(row[3])};				
				//String[] countsReadings = {"???", "!!!", "***"};  
				countsList.add(row);
			}
			csvReader.close();
		} catch (IOException e) {
			log.error("ERROR: Cannot read traffic counts file: " + this.turnRestrictionsFilename);
		} catch (NumberFormatException e) {
			log.error("ERROR: Check format of traffic counts file: " + this.turnRestrictionsFilename);
		}
		
		return countsList;
	}
	
	public void exportTrafficCounts(ArrayList<String[]>countsList, Map<Integer, ArrayList<Integer[]>> trafficCountersMap) {
        CSVWriter csvWriter = null;
		File f = new File(this.countsFilename);
		String filename = "";
		if (f.getParent() != null)
			filename = f.getParent() + "/";
		filename = filename + this.outputPrefix + f.getName();
		try {
			csvWriter = new CSVWriter(new FileWriter(filename));
			String header = "LinkID,CID,A,B,COUNTDATE,factor,AB0600,BA0600,AB0700,BA0700,AB0800,BA0800,AB0900,BA0900,AB1000,BA1000,AB1100,BA1100,AB1200,BA1200,AB1300,BA1300,AB1400,BA1400,AB1500,BA1500,AB1600,BA1600,AB1700,BA1700,AB1800,BA1800";
			csvWriter.writeNext(header.split(","), false);
			Integer cnt = 1;
			for(String[] originalCounts : countsList) {
				Integer nodeA = Integer.parseInt(originalCounts[2]);
				Integer nodeB = Integer.parseInt(originalCounts[3]);	
				// Check if the counts readings are affected by network changes
				// To fix the readings we need to split each row into 2 rows - one per direction
				// Change some fields: LinkID, CID, A, B, COUNTDATE, factor
				if (trafficCountersMap.containsKey(nodeA)) {
					// Node A was expanded
					for(Integer[] counts : trafficCountersMap.get(nodeA)) 
						if (counts[0].equals(nodeB)) {
							// Instead of A <-> B, we have now A_out <-> B and A_in <-> B							
							if (counts[1] != null) {								
								String[] readings = originalCounts.clone();
								readings[2] = counts[1].toString();
								for (int i = 6; i < 32; i = i + 2) {
									readings[i] = "0.0";
								}						
								readings[0] = cnt.toString();
								readings[1] = cnt.toString();
								cnt++;
								csvWriter.writeNext(readings, false);
							}
							if (counts[2] != null) {
								String[] readings = originalCounts.clone();
								readings[2] = counts[2].toString();
								for (int i = 7; i < 32; i = i + 2) {
									readings[i] = "0.0";
								}
								readings[0] = cnt.toString();
								readings[1] = cnt.toString();
								cnt++;
								csvWriter.writeNext(readings, false);
							}
						}
				} else if (trafficCountersMap.containsKey(nodeB)) {
					// Node B was expanded
					for(Integer[] counts : trafficCountersMap.get(nodeB)) 
						if (counts[0].equals(nodeA)) {
							// Instead of B <-> A, we have now B_in <-> A and B_out <-> A
							if (counts[1] != null) {								
								String[] readings = originalCounts.clone();
								readings[2] = counts[1].toString();
								for (int i = 7; i < 32; i = i + 2) {
									readings[i] = "0.0";
								}		
								readings[0] = cnt.toString();
								readings[1] = cnt.toString();
								cnt++;
								csvWriter.writeNext(readings, false);
							}
							if (counts[2] != null) {
								String[] readings = originalCounts.clone();
								readings[2] = counts[2].toString();
								for (int i = 6; i < 32; i = i + 2) {
									readings[i] = "0.0";
								}
								readings[0] = cnt.toString();
								readings[1] = cnt.toString();
								cnt++;
								csvWriter.writeNext(readings, false);
							}
						}
				} else {
					originalCounts[0] = cnt.toString();
					originalCounts[1] = cnt.toString();
					cnt++;
					csvWriter.writeNext(originalCounts, false);
				}			
			}
			csvWriter.close();
		} catch (IOException e) {
			log.error("ERROR: Cannot write traffic counts file: " + filename);
		} catch (NumberFormatException e) {
			log.error("ERROR: Check format traffic counts file: " + filename);
		}
	}
	
	public static void main(String[] args) {

		String turnRestrictionsFilename = "./data/EmmeManeuverRestrictions.csv";
		String nodesFilename = "./data/nodes.csv";
		String linksFilename = "./data/links.csv";
		String linepathsFilename = "./data/line_path.csv";
		String countsFilename = "./data/LinkCounts.csv";

		Turns2Network t2n = new Turns2Network(nodesFilename, linksFilename,
				linepathsFilename, turnRestrictionsFilename, countsFilename,
				"t_", 3, 2);
		t2n.buildNetwork();
	}

	/*
	 * Helper classes for nodes, links, and transit lines 
	 */
	
	final class Node {
		private Integer nodeId;
		private Double[] coord;
		
		public Node(Integer id, Double[] coord) {
			this.nodeId = id;
			this.coord = coord;
		}
		
		public Integer getId() {
			return this.nodeId; 
		}
		
		public Double[] getCoord() {
			return this.coord;
		}
		
		public Double getCoordX() {
			return this.coord[0];
		}
		
		public Double getCoordY() {
			return this.coord[1];
		}
		
		public String toString() {
			return this.nodeId + "(" + this.coord[0] + ", " + this.coord[1] + ")";
		}
	}
	
	final class Link {
		private Integer fromNode;
		private Integer toNode;
		private String length;
		private String mode;
		private String lanesNumber;
		private String type;
		private String at;
		private String capacity;
		private String speedLimit;

		public Link(String[] link) {
			this.fromNode = Integer.valueOf(link[0]);
			this.toNode = Integer.valueOf(link[1]);
			this.length = link[2];
			this.mode = link[3];
			this.lanesNumber = link[4];
			this.type = link[5];
			this.at = link[6];
			this.capacity = link[7];
			this.speedLimit = link[8];
		}

		public Integer getFromNode() {
			return this.fromNode;
		}

		public Integer getToNode() {
			return this.toNode;
		}
		
		public String getLength() {
			return this.length;
		}
		
		public String getId() {
			return this.fromNode + "_" + this.toNode + "_" + this.type;
		}
		
		public String[] getLinkRow() {
			String[] link = {
				this.fromNode.toString(),
				this.toNode.toString(),
				this.length,
				this.mode,
				this.lanesNumber,
				this.type,
				this.at,
				this.capacity,
				this.speedLimit
			};
			return link;
		}

		public String toString() {
			return String.join(", ", this.getLinkRow());
		}
	}
	
	final class LineSegment {
		private Integer fromNode;
		private Integer toNode;
		private String length;
		private String isStop;
		
		public LineSegment(Integer from, Integer to, String length, String isStop) {
			this.fromNode = from;
			this.toNode = to;
			this.length = length;
			this.isStop = isStop;
		}

		public String[] getSegmentRow() {
			String[] line = {
				this.fromNode.toString(),
				this.toNode.toString(),
				this.length,
				this.isStop
			};
			return line;
		}
		
		public Integer getFromNode() {
			return this.fromNode;
		}

		public Integer getToNode() {
			return this.toNode;
		}
		
		public String getLength() {
			return this.length;
		}
		
		public String getIsStop() {
			return this.isStop;
		}
		
		public String toString() {
			return String.join(", ", this.getSegmentRow());
		}
	}
	
	public final class Line {
		private final String lineId;
		private ArrayList<LineSegment> segments;
		
		public Line(String lineId) {
			this.lineId = lineId;
			this.segments = new ArrayList<LineSegment>();
		}
		
		public String getId() {
			return this.lineId;
		}

		public ArrayList<LineSegment> getSegments() {
			return this.segments;
		}
		
		public void addSegment(LineSegment newSegment) {
			this.segments.add(newSegment);
		} 
		
		public void addSegment(LineSegment newSegment, Integer entrySeqNumber) {	
			this.segments.add(entrySeqNumber, newSegment);	
		}

		public boolean containsTurn(Integer at, Integer from, Integer to) {
			for (int i = 0; i < this.segments.size()-1; i++) {
				Integer segAt = this.segments.get(i).toNode;
				Integer segFrom = this.segments.get(i).fromNode;
				Integer segTo = this.segments.get(i+1).toNode;
				if (segAt.equals(at) && segFrom.equals(from) && segTo.equals(to)) {
					return true;
				}				 
			}
			return false;
		}
		
		public void expandLineAtNode(Integer nodeId, 
				Map<Integer, Link> newInLinks,
				Map<Integer, Link> newOutLinks,
				Map<Integer[], Link> newConnectorLinks) {
			
			List<Integer> affectedInSegments = IntStream
					.range(0, this.segments.size())
					.filter(i -> this.segments.get(i).getToNode().equals(nodeId))
					.mapToObj(i -> i) 
					.collect(Collectors.toList());
			
			// Insert new segments for in-links
			for (Integer seqNumber : affectedInSegments) {
				Link newLink = newInLinks.get(this.segments.get(seqNumber).fromNode);
				if (newLink != null) {
					String isStop = this.segments.get(seqNumber).isStop;
					this.segments.remove((int)seqNumber);
					LineSegment newSegment = new LineSegment(newLink.getFromNode(), newLink.getToNode(), 
						newLink.getLength(), isStop);
					this.segments.add(seqNumber, newSegment);
				}							
			}
			
			List<Integer> affectedOutSegments = IntStream
					.range(0, this.segments.size())
					.filter(i -> this.segments.get(i).getFromNode().equals(nodeId))
					.mapToObj(i -> i) 
					.collect(Collectors.toList());
			
			// Insert new segments for out-links
			for (Integer seqNumber : affectedOutSegments) {
				Link newLink = newOutLinks.get(this.segments.get(seqNumber).toNode);
				if (newLink != null) {
					String isStop = this.segments.get(seqNumber).isStop;
					this.segments.remove((int)seqNumber);
					LineSegment newSegment = new LineSegment(newLink.getFromNode(), newLink.getToNode(), 
						newLink.getLength(), isStop);
					this.segments.add(seqNumber, newSegment);
				}					
			}

			// Find broken segments
			List<Integer> affectedInterSegments = IntStream
					.range(0, this.segments.size() - 1)
					.filter(i -> !this.segments.get(i).getToNode().equals(this.segments.get(i+1).getFromNode()))
					.mapToObj(i -> i) 
					.collect(Collectors.toList());

			// Repair broken segments
			Integer shift = 0; // Remember shift after every new portion of segments
			for (Integer seqNumber : affectedInterSegments) {
				seqNumber = seqNumber + shift;
				Integer toNode = this.segments.get(seqNumber+1).fromNode;
				Integer fromNode = this.segments.get(seqNumber).toNode; 
				
				List<Integer[]> lst = newConnectorLinks.keySet().stream()
						.filter(s -> s[0].equals(fromNode) && s[1].equals(toNode))
						.collect(Collectors.toList());
				
				if (lst.size() == 1) {
					Integer[] interKey = lst.get(0);
					Link newLink = newConnectorLinks.get(interKey);
					if (newLink != null) {
						LineSegment newSegment = new LineSegment(newLink.getFromNode(), newLink.getToNode(), 
							newLink.getLength(), "0");
						this.segments.add(seqNumber+1, newSegment);
						shift = shift + 1;
					}	
				}				
			}
		}
		
		public String toString() {
			String ret = this.lineId + "::[" + 
				this.segments.stream()
					.map(Object::toString)
					.collect(Collectors.joining(",\n"));
			return ret + "]\n"; 
		}
		
	}
}