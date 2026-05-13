import argparse
from typing import List, Optional


def add_numbers(first_number: float, second_number: float) -> float:
    return first_number + second_number


def parse_number(value: str) -> float:
    try:
        return float(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError(f"{value!r} is not a valid number") from error


def prompt_for_number(label: str) -> float:
    while True:
        value = input(f"Enter the {label} number: ").strip()
        if not value:
            print("Please enter a number.")
            continue

        try:
            return float(value)
        except ValueError:
            print(f"{value!r} is not a valid number. Try again.")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Add two numbers together.")
    parser.add_argument("numbers", nargs="*", type=parse_number, help="two numbers to add")
    return parser


def main(argv: Optional[List[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    if len(args.numbers) > 2:
        parser.error("please provide no more than two numbers")

    if len(args.numbers) == 2:
        first_number, second_number = args.numbers
    else:
        if args.numbers:
            first_number = args.numbers[0]
        else:
            first_number = prompt_for_number("first")
        second_number = prompt_for_number("second")

    total = add_numbers(first_number, second_number)
    print(f"The sum is: {total}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
