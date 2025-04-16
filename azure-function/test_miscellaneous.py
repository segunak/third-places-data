import os
import json
import csv
import datetime
import ast
import helper_functions as helpers
from collections import Counter
from constants import SearchField
from airtable_client import AirtableClient

def read_third_places_csv():
    """
    Read the Charlotte Third Places CSV file and convert it to a list of dictionaries
    where column names are keys and row values are the corresponding values.
    
    Returns:
        list: A list of dictionaries containing the CSV data
    """
    # Define the path to the CSV file
    csv_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data', 'Charlotte Third Places-All.csv')
    
    # Initialize the list to store the dictionaries
    places_data = []
    
    # Open and read the CSV file with utf-8-sig encoding to handle BOM character
    with open(csv_path, 'r', encoding='utf-8-sig') as csv_file:
        # Create a CSV reader with DictReader which automatically uses the first row as keys
        csv_reader = csv.DictReader(csv_file)
        
        # Add each row to the list as a dictionary
        for row in csv_reader:
            places_data.append(row)
    
    return places_data

def select_prioritized_photos(photos_data, max_photos=25):
    """
    Selects photos based on specific criteria from the provided photos data.
    
    Selection criteria:
    1. First sort all photos by date, newest to oldest
    2. Select photos with the following priority:
       - "vibe" tag: Include all vibe photos possible
       - "front" tag: Up to 5 photos (take all available if less than 5)
       - "all" and "other" tags: Fill remaining slots after vibe and front
    3. Return a maximum of 'max_photos' photo URLs (default: 25)
    4. If the total count of desired tags is less than max_photos, include all of them
    
    Args:
        photos_data (list or str): List of photo dictionaries or a JSON string representing this list
        max_photos (int): Maximum number of photos to select (default: 25)
    
    Returns:
        tuple: (list of selected photo URLs, dict of tag counts in selected photos)
    """
    # Handle case where photos_data is empty or None
    if not photos_data:
        return [], {}
    
    # Parse the photos data if it's a string
    if isinstance(photos_data, str):
        try:
            # Use ast.literal_eval to safely parse the string representation
            photos_data = ast.literal_eval(photos_data)
        except (SyntaxError, ValueError) as e:
            print(f"Error: Could not parse photos data: {e}")
            return [], {}
    
    # If photos_data is still not a list or is empty, return empty list
    if not isinstance(photos_data, list) or not photos_data:
        return [], {}
    
    # Helper function to parse date strings
    def parse_date(date_str):
        try:
            # Try to parse the date format "MM/DD/YYYY HH:MM:SS"
            return datetime.datetime.strptime(date_str, "%m/%d/%Y %H:%M:%S")
        except ValueError:
            # If parsing fails, return the earliest possible date
            return datetime.datetime.min
    
    # Sort photos by date, newest first
    photos_data.sort(key=lambda x: parse_date(x.get('photo_date', '')), reverse=True)
    
    # Initialize collections for different photo categories
    front_photos = []
    vibe_photos = []
    all_photos = []
    other_photos = []
    remaining_photos = []
    
    # Categorize photos based on tags
    for photo in photos_data:
        # Skip photos without a photo_url_big
        if 'photo_url_big' not in photo:
            continue
        
        tags = photo.get('photo_tags', [])
        
        # Skip photos without any tags
        if not isinstance(tags, list) or not tags:
            continue
            
        # Categorize photos by priority tags
        if 'front' in tags:
            front_photos.append(photo)
        elif 'vibe' in tags:
            vibe_photos.append(photo)
        elif 'all' in tags:
            all_photos.append(photo)
        elif 'other' in tags:
            other_photos.append(photo)
        else:
            remaining_photos.append(photo)
    
    # Count the total preferred photos (front, vibe, all, other)
    total_preferred = len(front_photos) + len(vibe_photos) + len(all_photos) + len(other_photos)
    
    # If total preferred photos is less than max_photos, take all of them
    if total_preferred <= max_photos:
        selected_photos = front_photos + vibe_photos + all_photos + other_photos
    else:
        # Otherwise apply the priority rules with limits
        selected_photos = []
        
        # First priority: vibe photos (all of them, limited by max_photos)
        vibe_limit = min(len(vibe_photos), max_photos)
        selected_photos.extend(vibe_photos[:vibe_limit])
        
        # Second priority: front photos (up to 5)
        remaining_slots = max_photos - len(selected_photos)
        front_limit = min(5, len(front_photos), remaining_slots)
        selected_photos.extend(front_photos[:front_limit])
        
        # Next priority: all photos 
        remaining_slots = max_photos - len(selected_photos)
        selected_photos.extend(all_photos[:remaining_slots])
        
        # Last priority: other photos
        remaining_slots = max_photos - len(selected_photos)
        selected_photos.extend(other_photos[:remaining_slots])
    
    # If we still have room, add remaining photos
    remaining_slots = max_photos - len(selected_photos)
    if remaining_slots > 0:
        selected_photos.extend(remaining_photos[:remaining_slots])
    
    # Remove duplicates while preserving order (in case a photo has multiple tags)
    unique_photos = []
    seen_urls = set()
    
    for photo in selected_photos:
        url = photo['photo_url_big']
        if url not in seen_urls:
            unique_photos.append(photo)
            seen_urls.add(url)
    
    # Track the tags that were selected
    selected_tag_counts = Counter()
    for photo in unique_photos[:max_photos]:
        # Count each tag in the photo
        for tag in photo.get('photo_tags', []):
            selected_tag_counts[tag] += 1
    
    # Extract photo_url_big values from the selected photos
    selected_urls = [photo['photo_url_big'] for photo in unique_photos[:max_photos]]
    
    return selected_urls, dict(selected_tag_counts)

def analyze_photo_tags(photos_data):
    """
    Analyzes the frequency of each tag in the photos data.
    
    Args:
        photos_data (list or str): List of photo dictionaries or a JSON string representing this list
        
    Returns:
        dict: A dictionary with the count of each tag
    """
    # Parse the photos data if it's a string
    if isinstance(photos_data, str):
        try:
            photos_data = ast.literal_eval(photos_data)
        except (SyntaxError, ValueError) as e:
            print(f"Error: Could not parse photos data for tag analysis: {e}")
            return {"error": str(e)}
    
    # If photos_data is not a list or is empty, return empty result
    if not isinstance(photos_data, list) or not photos_data:
        return {"error": "No photos data or invalid format"}
    
    # Collect all tags
    all_tags = []
    for photo in photos_data:
        tags = photo.get('photo_tags', [])
        if isinstance(tags, list):
            all_tags.extend(tags)
    
    # Count the frequency of each tag
    tag_counts = dict(Counter(all_tags))
    
    # Calculate the total number of photos
    total_photos = len(photos_data)
    
    return {
        "total_photos": total_photos,
        "tag_counts": tag_counts
    }

def find_matching_airtable_records():
    """
    Find matching records in Airtable for each place in the CSV file
    using Google Maps Place Id as the matching key.
    Then update the "Photos" field in Airtable with the selected photos.
    
    Returns:
        dict: A dictionary containing match results
    """
    # Read data from CSV file
    places_from_csv = read_third_places_csv()
    
    # Initialize AirtableClient
    airtable_client = AirtableClient()
    
    # Initialize results
    results = {
        "matched": [],
        "not_matched": [],
        "no_place_id": [],
        "tag_analysis": {},  # New field for tag analysis results
        "airtable_updates": []  # Track Airtable update results
    }
    
    # Process each place from CSV
    total_processed = 0
    for place in places_from_csv:
        total_processed += 1
        place_name = place.get('Place', 'Unknown')
        place_id = place.get('Google Maps Place Id')
        
        if not place_id:
            results["no_place_id"].append(place_name)
            continue
        
        # Find matching record in Airtable
        record = airtable_client.get_record(SearchField.GOOGLE_MAPS_PLACE_ID, place_id)
        
        if record:
            # Process photos using our prioritization algorithm
            photos_data = place.get('Photos Outscraper Reference', [])
            
            # Analyze photo tags for this place
            tag_analysis = analyze_photo_tags(photos_data)
            results["tag_analysis"][place_name] = tag_analysis
            
            # Select photos based on priority algorithm
            selected_photo_urls, selected_tag_counts = select_prioritized_photos(photos_data)
            
            # Add to matched results with selected photos
            results["matched"].append({
                "csv_name": place_name,
                "airtable_name": record['fields'].get('Place', 'Unknown'),
                "place_id": place_id,
                "airtable_record_id": record['id'],
                "selected_photos": selected_photo_urls,
                "tag_analysis": tag_analysis,
                "selected_tag_counts": selected_tag_counts
            })
            
            # Update the "Photos" field in Airtable with the selected photos
            # Format the selected photos as a string representation of a list for storage
            if selected_photo_urls:
                # Convert the list to a string representation for Airtable storage
                photo_list_string = str(selected_photo_urls)
                
                # Update the Airtable record with the selected photos
                # Set overwrite=False to avoid overwriting if the field already has value
                update_result = airtable_client.update_place_record(
                    record_id=record['id'],
                    field_to_update="Photos",
                    update_value=photo_list_string,
                    overwrite=False
                )
                
                # Log the update result
                if update_result["updated"]:
                    print(f"Updated 'Photos' field for '{place_name}' with {len(selected_photo_urls)} selected photos")
                else:
                    print(f"Skipped updating 'Photos' field for '{place_name}' - already has values or no change needed")
                
                # Store the update result in our results dictionary
                results["airtable_updates"].append({
                    "place_name": place_name,
                    "record_id": record['id'],
                    "photos_count": len(selected_photo_urls),
                    "updated": update_result["updated"]
                })
            
            # Show minimal progress output
            if total_processed % 10 == 0:
                print(f"Processed {total_processed}/{len(places_from_csv)} places...")
        else:
            results["not_matched"].append({
                "name": place_name,
                "place_id": place_id
            })
    
    # Generate a summary of all photo tags used across all places
    all_places_tag_counter = Counter()
    for place_name, analysis in results["tag_analysis"].items():
        tag_counts = analysis.get("tag_counts", {})
        for tag, count in tag_counts.items():
            all_places_tag_counter[tag] += count
    
    # Add overall tag analysis to the results
    results["overall_tag_analysis"] = dict(all_places_tag_counter)
    
    # Print summary of Airtable updates
    total_updated = sum(1 for update in results["airtable_updates"] if update["updated"])
    print(f"\nAirtable Update Summary:")
    print(f"  Total records processed: {len(results['airtable_updates'])}")
    print(f"  Records updated: {total_updated}")
    print(f"  Records skipped: {len(results['airtable_updates']) - total_updated}")
    
    return results

def save_results_to_file(results):
    """
    Saves the analysis results to files in the data/analysis folder.
    
    Args:
        results (dict): The results to save
    """
    # Create data/analysis directory if it doesn't exist
    data_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data', 'analysis')
    os.makedirs(data_dir, exist_ok=True)
    
    # Save full results as JSON
    json_path = os.path.join(data_dir, 'photo_tag_analysis_results.json')
    with open(json_path, 'w', encoding='utf-8') as f:
        json.dump(results, f, indent=2)
    
    # Create a markdown report that's more human-readable
    md_path = os.path.join(data_dir, 'photo_tag_analysis_report.md')
    with open(md_path, 'w', encoding='utf-8') as f:
        f.write("# Photo Tag Analysis Report\n\n")
        
        # Overall summary
        f.write("## Summary\n")
        f.write(f"- Total places in CSV: {len(results.get('matched', [])) + len(results.get('not_matched', [])) + len(results.get('no_place_id', []))}\n")
        f.write(f"- Places with matched records: {len(results.get('matched', []))}\n")
        f.write(f"- Places without matched records: {len(results.get('not_matched', []))}\n")
        f.write(f"- Places without Place ID: {len(results.get('no_place_id', []))}\n\n")
        
        # Overall tag analysis
        f.write("## Overall Tag Frequency\n")
        overall_tags = results.get('overall_tag_analysis', {})
        if overall_tags:
            f.write("| Tag | Count |\n")
            f.write("|-----|-------|\n")
            for tag, count in sorted(overall_tags.items(), key=lambda x: x[1], reverse=True):
                f.write(f"| {tag} | {count} |\n")
        else:
            f.write("No tag data available.\n")
        f.write("\n")
        
        # Per-place tag analysis
        f.write("## Per-Place Tag Analysis\n\n")
        
        for place in results.get('matched', []):
            place_name = place.get('csv_name', 'Unknown')
            f.write(f"### {place_name}\n")
            
            tag_analysis = place.get('tag_analysis', {})
            total_photos = tag_analysis.get('total_photos', 0)
            f.write(f"- Total photos: {total_photos}\n")
            
            # Add selected photo count
            selected_photos = place.get('selected_photos', [])
            f.write(f"- Selected photos: {len(selected_photos)}\n")
            
            tag_counts = tag_analysis.get('tag_counts', {})
            if tag_counts:
                f.write("- Tag distribution:\n")
                f.write("  | Tag | Count | Percentage |\n")
                f.write("  |-----|-------|------------|\n")
                for tag, count in sorted(tag_counts.items(), key=lambda x: x[1], reverse=True):
                    percentage = round((count / total_photos) * 100, 1) if total_photos > 0 else 0
                    f.write(f"  | {tag} | {count} | {percentage}% |\n")
            else:
                f.write("- No tag data available.\n")
            
            f.write("\n")
    
    print(f"Analysis complete. Files saved to:")
    print(f"- JSON data: {json_path}")
    print(f"- Markdown report: {md_path}")

# # Print the number of places from CSV file
# places_csv = read_third_places_csv()
# print(f"Found {len(places_csv)} places in the CSV file")

# # Call the function to find matching records in Airtable
# print("\nMatching CSV records with Airtable records...")
# match_results = find_matching_airtable_records()

# # Save the results to files
# save_results_to_file(match_results)

airtable = helpers.get_airtable_client(debug_mode=True)
airtable.enrich_base_data()
print("Base data enrichment complete.")
