import argparse

class ArgsBuilder:
    @staticmethod
    def build_args():
        parser = argparse.ArgumentParser(description="Process some integers.")
        parser.add_argument('--input', type=str, required=True, help='Input file path')
        parser.add_argument('--is_local', type=bool, required=True, help='If the input is local')
        parser.add_argument('--output', type=str, required=True, help='Output file path')
        parser.add_argument('--start-date', type=str, required=True, help='Start date in YYYY-MM-DD format')
        parser.add_argument('--end-date', type=str, required=True, help='End date in YYYY-MM-DD format')
        return parser.parse_args()