import argparse
import os
from rosbags.rosbag2 import Reader
from rosbags.serde import deserialize_cdr

import datetime

def parse_ros2_bag(bag_path):
    """
    Parses a ROS2 bag file, displays its size, topics, and message counts.

    Args:
        bag_path (str): Path to the ROS2 bag directory or a .db3 file within it.
    """
    original_bag_path = bag_path
    if not os.path.exists(bag_path):
        print(f"Error: Path '{bag_path}' does not exist.")
        return

    # If a .db3 file is provided, use its parent directory as the bag path
    if os.path.isfile(bag_path) and bag_path.endswith('.db3'):
        bag_file_name = os.path.basename(bag_path)
        bag_path = os.path.dirname(bag_path)
        print(f"Warning: '.db3' file provided. Using parent directory as bag path: '{bag_path}'")
    elif os.path.isdir(bag_path):
        # Find the .db3 file within the directory
        db3_files = [f for f in os.listdir(bag_path) if f.endswith('.db3')]
        if not db3_files:
            print(f"Error: No '.db3' file found in '{bag_path}'.")
            return
        bag_file_name = db3_files # Assuming one .db3 file per bag
    else:
        print(f"Error: '{original_bag_path}' is not a valid ROS2 bag directory or a .db3 file.")
        return

    if not os.path.isdir(bag_path):
        print(f"Error: '{bag_path}' is not a valid ROS2 bag directory.")
        return

    print(f"Parsing ROS2 bag from: {bag_path}")

    try:
        with Reader(bag_path) as reader:
            # Get bag file size
            total_size = 0
            for dirpath, dirnames, filenames in os.walk(bag_path):
                for f in filenames:
                    fp = os.path.join(dirpath, f)
                    total_size += os.path.getsize(fp)

            # Convert bytes to MiB
            bag_size_mib = total_size / (1024 * 1024)

            # Get metadata
            metadata = reader.metadata

            # Calculate duration
            duration_ns = metadata['duration']['nanoseconds']
            duration_s = duration_ns / 1e9

            # Convert start/end timestamps
            start_time_ns = metadata['starting_time']['nanoseconds_since_epoch']
            end_time_ns = start_time_ns + duration_ns
            start_datetime = datetime.datetime.fromtimestamp(start_time_ns / 1e9)
            end_datetime = datetime.datetime.fromtimestamp(end_time_ns / 1e9)

            # Total messages
            total_messages = metadata['message_count']
            print(f"Files:             {bag_file_name}")
            print(f"Bag size:          {bag_size_mib:.1f} MiB")
            print(f"Storage id:        {metadata['storage_identifier']}")
            print(f"Duration:          {duration_s:.3f}s")
            print(f"Start:             {start_datetime.strftime('%b %d %Y %H:%M:%S.%f')[:-3]} ({start_time_ns / 1e9:.3f})")
            print(f"End:               {end_datetime.strftime('%b %d %Y %H:%M:%S.%f')[:-3]} ({end_time_ns / 1e9:.3f})")
            print(f"Messages:          {total_messages}")
            print("Topic information:")
            
            topic_message_counts = {item['topic_metadata']['name']: item['message_count'] for item in metadata['topics_with_message_count']}

            #print(f"DEBUG: connection object: {reader.connections}")
            for connection in reader.connections:
                topic_name = connection.topic
                message_count = topic_message_counts.get(topic_name, 0) # Default to 0 if not found
                print(f"  Topic: {topic_name} | Type: {connection.msgtype} | Count: {message_count} | Serialization Format: {connection.ext.serialization_format}")

    except Exception as e:
        print(f"Error parsing bag file: {e}")

def main():
    parser = argparse.ArgumentParser(description="Parse ROS2 bag files and display their information.")
    parser.add_argument("bag_path", type=str, help="Path to the ROS2 bag directory or a .db3 file within it.")
    args = parser.parse_args()

    parse_ros2_bag(args.bag_path)

if __name__ == "__main__":
    main()

