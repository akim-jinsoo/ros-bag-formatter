"""Convert ROS 2 bag files to CSV format, creating one CSV file per topic."""

import argparse
import os
import sys
from pathlib import Path

from rclpy.serialization import deserialize_message
from rosidl_runtime_py.utilities import get_message

# This is needed on Linux when compiling with clang/libc++.
# TL;DR This makes class_loader work when using a python extension compiled with libc++.
# For the fun RTTI ABI details, see https://whatofhow.wordpress.com/2015/03/17/odr-rtti-dso/.
if os.environ.get('ROSBAG2_PY_TEST_WITH_RTLD_GLOBAL') is not None:
    sys.setdlopenflags(os.RTLD_GLOBAL | os.RTLD_LAZY)

import rosbag2_py  # noqa: E402


def get_rosbag_options(path, serialization_format='cdr'):
    """Create storage and converter options for reading a ROS bag.
    
    Args:
        path: Path to the ROS bag directory.
        serialization_format: Message serialization format (default: 'cdr').
    
    Returns:
        Tuple of (storage_options, converter_options).
    """
    storage_options = rosbag2_py.StorageOptions(uri=path, storage_id='sqlite3')
    converter_options = rosbag2_py.ConverterOptions(
        input_serialization_format=serialization_format,
        output_serialization_format=serialization_format
    )
    return storage_options, converter_options


def _gen_msg_values(msg, prefix=""):
    """Recursively extract field names and values from a ROS message.
    
    Args:
        msg: ROS message object or primitive value.
        prefix: Field name prefix for nested fields.
    
    Yields:
        Tuples of (field_name, field_value) for each primitive field.
    """
    if isinstance(msg, list):
        for i, val in enumerate(msg):
            yield from _gen_msg_values(val, f"{prefix}[{i}]")
    elif hasattr(msg, "get_fields_and_field_types"):
        for field, field_type in msg.get_fields_and_field_types().items():
            val = getattr(msg, field)
            full_field_name = f"{prefix}.{field}" if prefix else field
            if field_type.startswith("sequence<"):
                for i, array_val in enumerate(val):
                    yield from _gen_msg_values(array_val, f"{full_field_name}[{i}]")
            else:
                yield from _gen_msg_values(val, full_field_name)
    else:
        yield prefix, msg


def dump_bag(bag_path, output_path):
    """Convert ROS bag to CSV files, one per topic.
    
    Args:
        bag_path: Path to the ROS bag directory.
        output_path: Path where CSV files will be saved.
    """
    storage_options, converter_options = get_rosbag_options(bag_path)

    reader = rosbag2_py.SequentialReader()
    reader.open(storage_options, converter_options)

    topic_types = reader.get_all_topics_and_types()

    # Create a map for quicker lookup
    type_map = {topic.name: topic.type for topic in topic_types}

    file_map = {}
    start_time = None
    msg_count = 0
    
    SKIP_TOPICS = {"/rosout", "/parameter_events"}
    
    # Ensure output directory exists
    os.makedirs(output_path, exist_ok=True)
    
    while reader.has_next():
        topic, data, timestamp = reader.read_next()
        
        if topic in SKIP_TOPICS:
            continue
            
        msg_type = get_message(type_map[topic])
        msg = deserialize_message(data, msg_type)

        # Create CSV file for new topic
        if topic not in file_map:
            csv_filename = f"{output_path}/{topic.lstrip('/').replace('/', '_')}.csv"
            file = open(csv_filename, "w")
            
            fields = [field for field, _ in _gen_msg_values(msg)
                      if not field.startswith("header.")]
            print("time," + ",".join(fields), file=file)
            file_map[topic] = file

        file = file_map[topic]
        
        # Extract timestamp from message header if available, otherwise use bag timestamp
        if hasattr(msg, "header"):
            time_sec = msg.header.stamp.sec + 1e-9 * msg.header.stamp.nanosec
        else:
            time_sec = timestamp
            
        if start_time is None:
            start_time = time_sec
            
        relative_time = time_sec - start_time
        values = [str(val) for _, val in _gen_msg_values(msg)
                  if not _.startswith("header.")]
        print(",".join([str(relative_time)] + values), file=file)
        
        if msg_count % 1000 == 0:
            print(f"  {relative_time:5.3f}s")
        msg_count += 1
    
    # Close all CSV files
    for file in file_map.values():
        file.close()
    
    return len(file_map)


def is_rosbag_dir(path):
    """Check if a directory is a ROS bag directory.
    
    A directory is considered a ROS bag if it contains metadata.yaml and .db3 files.
    
    Args:
        path: Path to check.
    
    Returns:
        True if the directory is a ROS bag, False otherwise.
    """
    path = Path(path)
    if not path.is_dir():
        return False
    
    has_metadata = (path / "metadata.yaml").exists()
    has_db3 = any(path.glob("*.db3"))
    
    return has_metadata and has_db3


def find_rosbags(root_path):
    """Recursively find all ROS bag directories under a given path.
    
    Args:
        root_path: Root directory to search.
    
    Returns:
        List of Path objects pointing to ROS bag directories.
    """
    root_path = Path(root_path)
    rosbags = []
    
    for dirpath, dirnames, filenames in os.walk(root_path):
        dirpath = Path(dirpath)
        if is_rosbag_dir(dirpath):
            rosbags.append(dirpath)
            # Don't search subdirectories of a rosbag
            dirnames.clear()
    
    return sorted(rosbags)


def get_relative_structure(bag_path, root_path):
    """Get the relative path structure from root to bag.
    
    Args:
        bag_path: Path to the ROS bag directory.
        root_path: Root directory path.
    
    Returns:
        Relative path from root to bag.
    """
    return Path(bag_path).relative_to(Path(root_path))


def process_single_bag(bag_path, output_path):
    """Process a single ROS bag file.
    
    Args:
        bag_path: Path to the ROS bag directory.
        output_path: Path where CSV files will be saved.
    """
    print(f"Processing: {bag_path}")
    topic_count = dump_bag(str(bag_path), str(output_path))
    print(f"  ✓ Created {topic_count} CSV file(s) in {output_path}\n")


def process_structured_bags(input_root):
    """Process ROS bags in a structured directory with ros2bag and csv folders.
    
    Args:
        input_root: Root directory containing ros2bag and csv folders.
    """
    input_root = Path(input_root)
    ros2bag_dir = input_root / "ros2bag"
    csv_dir = input_root / "csv"
    
    if not ros2bag_dir.exists():
        print(f"Error: 'ros2bag' directory not found in {input_root}", file=sys.stderr)
        sys.exit(1)
    
    # Create csv directory if it doesn't exist
    csv_dir.mkdir(exist_ok=True)
    
    # Find all rosbag directories
    rosbags = find_rosbags(ros2bag_dir)
    
    if not rosbags:
        print(f"No ROS bags found in {ros2bag_dir}", file=sys.stderr)
        sys.exit(1)
    
    print(f"Found {len(rosbags)} ROS bag(s) to process\n")
    
    for bag_path in rosbags:
        # Get relative path structure and create corresponding CSV output path
        rel_path = get_relative_structure(bag_path, ros2bag_dir)
        output_path = csv_dir / rel_path
        
        process_single_bag(bag_path, output_path)
    
    print(f"✓ All bags processed successfully!")


def main():
    """Main entry point for the script."""
    parser = argparse.ArgumentParser(
        description="Convert ROS 2 bag files to CSV format.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Convert a single ROS bag
  %(prog)s /path/to/single/rosbag

  # Process structured directory with ros2bag/ and csv/ folders
  %(prog)s /path/to/project
  
  The structured mode expects:
    project/
      ros2bag/
        config1/
          trial_1/  (rosbag)
          trial_2/  (rosbag)
        config2/
          trial_1/  (rosbag)
      csv/         (created automatically with same structure)
        """
    )
    
    parser.add_argument(
        'input',
        help='Path to a single ROS bag directory or root directory containing ros2bag folder'
    )
    
    args = parser.parse_args()
    
    input_path = Path(args.input)
    
    if not input_path.exists():
        print(f"Error: Path '{input_path}' does not exist.", file=sys.stderr)
        sys.exit(1)
    
    if not input_path.is_dir():
        print(f"Error: '{input_path}' is not a directory.", file=sys.stderr)
        sys.exit(1)
    
    # Check if this is a single rosbag or structured directory
    if is_rosbag_dir(input_path):
        # Single ROS bag - output CSVs in the same directory
        process_single_bag(input_path, input_path)
    elif (input_path / "ros2bag").exists():
        # Structured directory with ros2bag folder
        process_structured_bags(input_path)
    else:
        # Check if input contains rosbags directly
        rosbags = find_rosbags(input_path)
        if rosbags:
            print(f"Found {len(rosbags)} ROS bag(s) in {input_path}")
            print("Note: No 'ros2bag' folder found. Creating 'csv' folder in same location.\n")
            
            csv_dir = input_path / "csv"
            csv_dir.mkdir(exist_ok=True)
            
            for bag_path in rosbags:
                rel_path = get_relative_structure(bag_path, input_path)
                output_path = csv_dir / rel_path
                process_single_bag(bag_path, output_path)
            
            print(f"✓ All bags processed successfully!")
        else:
            print(f"Error: No ROS bags found in '{input_path}'", file=sys.stderr)
            print("Expected either:", file=sys.stderr)
            print("  - A single ROS bag directory (with metadata.yaml and .db3 files)", file=sys.stderr)
            print("  - A directory with a 'ros2bag' subdirectory", file=sys.stderr)
            print("  - A directory containing ROS bag subdirectories", file=sys.stderr)
            sys.exit(1)


if __name__ == "__main__":
    main()