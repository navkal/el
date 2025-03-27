import csv
import sys
from xml.etree.ElementTree import Element, SubElement, ElementTree

def convert_hex_to_kml_color(hex_color, alpha="BF"):
    """Convert hex color to KML color format."""
    hex_color = hex_color.lstrip("#")
    if len(hex_color) != 6:
        return "BFFFFFFF"  # Default to transparent white if invalid
    r, g, b = hex_color[:2], hex_color[2:4], hex_color[4:6]
    return f"{alpha}{b}{g}{r}"

def create_kml(input_csv, output_kml):
    try:
        # Create KML root element
        kml = Element("kml", xmlns="http://www.opengis.net/kml/2.2")
        document = SubElement(kml, "Document")
        
        # Read the CSV file and create placemarks
        with open(input_csv, newline='') as csvfile:
            reader = csv.reader(csvfile)
            for row in reader:
                if len(row) != 7:
                    print(f"Skipping invalid row: {row}")
                    continue  # Skip invalid rows
                try:
                    title, shape, lat, lon, color, radius, link = map(str.strip, row)
                    lat, lon = float(lat), float(lon)
                    
                    kml_color = convert_hex_to_kml_color(color, "BF")  # 25% transparency
                    
                    placemark = SubElement(document, "Placemark")
                    
                    # Set the title as the mouse-over text
                    name = SubElement(placemark, "name")
                    name.text = ""  # Ensure mouse-over displays the first element in the CSV
                    
                    # Display the first item in the list on hover with clickable link
                    description = SubElement(placemark, "description")
                    description.text = f"<![CDATA[{title}<br><a href='{link}' target='_blank'>Property Info</a>"
                    
                    style = SubElement(placemark, "Style")
                    icon_style = SubElement(style, "IconStyle")
                    icon_color_element = SubElement(icon_style, "color")
                    icon_color_element.text = kml_color
                    icon_element = SubElement(icon_style, "Icon")
                    href = SubElement(icon_element, "href")
                    
                    # Use Google's built-in shape icons
                    if shape.lower() == "circle":
                        href.text = "http://maps.google.com/mapfiles/kml/shapes/placemark_circle.png"
                    elif shape.lower() == "square":
                        href.text = "http://maps.google.com/mapfiles/kml/shapes/placemark_square.png"
                    elif shape.lower() == "star":
                        href.text = "http://maps.google.com/mapfiles/kml/shapes/star.png"  # Replaced triangle with star
                    else:
                        print(f"Invalid shape type: {shape}, skipping.")
                        continue
                    
                    # Add a point for visualization
                    point = SubElement(placemark, "Point")
                    point_coordinates = SubElement(point, "coordinates")
                    point_coordinates.text = f"{lon},{lat},0"
                
                except ValueError as e:
                    print(f"Error processing row {row}: {e}")
        
        # Write to KML file
        tree = ElementTree(kml)
        with open(output_kml, "wb") as kmlfile:
            tree.write(kmlfile, encoding="utf-8", xml_declaration=True)
        print(f"KML file written to {output_kml}")
    except Exception as e:
        print(f"An error occurred: {e}")

def main():
    if len(sys.argv) != 3:
        print("Usage: python generate_kml.py <input_csv> <output_kml>")
        sys.exit(1)

    input_csv = sys.argv[1]
    output_kml = sys.argv[2]
    create_kml(input_csv, output_kml)

if __name__ == "__main__":
    main()
