import argparse
import os
from rosbags.rosbag2 import Reader
from rosbags.serde import deserialize_cdr

import datetime
import json
import time
from dora import DoraStatus, Operator

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

            # Extract IMU data
            extracted_data = {
                "/Imu": [],
                "/odom": [],
            }
            for connection, timestamp, rawdata in reader.messages():
                if connection.topic == '/Imu':
                    msg = deserialize_cdr(rawdata, connection.msgtype)
                    extracted_data["/Imu"].append({
                        "timestamp": timestamp,
                        "orientation": {
                            "x": msg.orientation.x,
                            "y": msg.orientation.y,
                            "z": msg.orientation.z,
                            "w": msg.orientation.w,
                        },
                        "angular_velocity": {
                            "x": msg.angular_velocity.x,
                            "y": msg.angular_velocity.y,
                            "z": msg.angular_velocity.z,
                        },
                        "linear_acceleration": {
                            "x": msg.linear_acceleration.x,
                            "y": msg.linear_acceleration.y,
                            "z": msg.linear_acceleration.z,
                        },
                    })
                elif connection.topic == '/odom':
                    msg = deserialize_cdr(rawdata, connection.msgtype)
                    extracted_data["/odom"].append({
                        "timestamp": timestamp,
                        "pose": {
                            "pose": {
                                "position": {
                                    "x": msg.pose.pose.position.x,
                                    "y": msg.pose.pose.position.y,
                                    "z": msg.pose.pose.position.z,
                                },
                                "orientation": {
                                    "x": msg.pose.pose.orientation.x,
                                    "y": msg.pose.pose.orientation.y,
                                    "z": msg.pose.pose.orientation.z,
                                    "w": msg.pose.pose.orientation.w,
                                },
                            },
                            "covariance": list(msg.pose.covariance),
                        },
                        "twist": {
                            "twist": {
                                "linear": {
                                    "x": msg.twist.twist.linear.x,
                                    "y": msg.twist.twist.linear.y,
                                    "z": msg.twist.twist.linear.z,
                                },
                                "angular": {
                                    "x": msg.twist.twist.angular.x,
                                    "y": msg.twist.twist.angular.y,
                                    "z": msg.twist.twist.angular.z,
                                },
                            },
                            "covariance": list(msg.twist.covariance),
                        },
                    })
            print(f"\nExtracted {len(extracted_data['/Imu'])} IMU messages.")
            print(f"Extracted {len(extracted_data['/odom'])} Odom messages.")
            return extracted_data

    except Exception as e:
        print(f"Error parsing bag file: {e}")
        return None

class DoraBagOperator(Operator):
    def __init__(self):
        self.all_data = {
            "/Imu": [],
            "/odom": [],
        }
        self.imu_index = 0
        self.odom_index = 0
        self.last_send_time = time.time()
        self.send_frequency = 10 # Hz, default frequency

    def on_event(
        self,
        dora_event: dict,
        send_output,
    ) -> DoraStatus:
        event_type = dora_event["type"]
        if event_type == "INPUT":
            pass
        elif event_type == "TIMER_TICK":
            current_time = time.time()
            if current_time - self.last_send_time >= 1.0 / self.send_frequency:
                # Send IMU data
                if self.imu_index < len(self.all_data["/Imu"]):
                    imu_message = self.all_data["/Imu"][self.imu_index]
                    json_message = json.dumps(imu_message)
                    send_output(
                        "imu_json",
                        json_message.encode(),
                        dora_event["metadata"],
                    )
                    self.imu_index += 1
                
                # Send Odom data
                if self.odom_index < len(self.all_data["/odom"]):
                    odom_message = self.all_data["/odom"][self.odom_index]
                    json_message = json.dumps(odom_message)
                    send_output(
                        "odom_json",
                        json_message.encode(),
                        dora_event["metadata"],
                    )
                    self.odom_index += 1

                self.last_send_time = current_time

                if self.imu_index >= len(self.all_data["/Imu"]) and \
                   self.odom_index >= len(self.all_data["/odom"]):
                    print("Finished sending all IMU and Odom data.")
                    return DoraStatus.STOP
        elif event_type == "STOP":
            print("received stop")
        else:
            print("received unexpected event:", event_type)

        return DoraStatus.CONTINUE

def main():
    parser = argparse.ArgumentParser(description="Parse ROS2 bag files and display their information.")
    parser.add_argument("bag_path", type=str, help="Path to the ROS2 bag directory or a .db3 file within it.")
    parser.add_argument("--frequency", type=int, default=10, help="Frequency to send IMU and Odom data in Hz.")
    args = parser.parse_args()

    extracted_data = parse_ros2_bag(args.bag_path)
    if extracted_data:
        print("\nIMU and Odom data extracted and ready to be sent to DORA.")
        operator = DoraBagOperator()
        operator.all_data = extracted_data
        operator.send_frequency = args.frequency
        
        print(f"Simulating DORA dataflow with IMU and Odom data at {args.frequency} Hz...")
        while operator.on_event({"type": "TIMER_TICK", "metadata": {}},
                                 lambda output_id, data, metadata: print(f"Sending {output_id}: {data.decode()}")) == DoraStatus.CONTINUE:
            time.sleep(1.0 / args.frequency)

if __name__ == "__main__":
    main()


 # 把 parse_ros2_bag 封装为一个文件，文件的功能就是解析ROS bag，输出指定话题的数据，传入参数是bag_path 和话题名
 # 根据话题的类型 自动匹配解析函数（IMU  里程计 GNSS  图像 激光点云 位姿pose 轨迹path）