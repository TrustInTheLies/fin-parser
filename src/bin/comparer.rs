use std::fmt::Display;

use fin_parser::{Format, Record};

struct Compared {
    input: String,
    format: Format,
}

struct Args {
    first: Compared,
    second: Compared,
}

#[derive(Debug)]
enum ArgsError {
    WrongCount(String),
    WrongArg(String),
}

impl Display for ArgsError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ArgsError::WrongCount(v) => write!(f, "{}", v),
            ArgsError::WrongArg(v) => write!(f, "{}", v),
        }
    }
}

impl Args {
    fn new() -> Result<Self, ArgsError> {
        let mut args = std::env::args().collect::<Vec<String>>();
        args.remove(0);
        if args.len() != 8 {
            return Err(ArgsError::WrongCount("Wrong number of arguments".into()));
        }
        if args[0] != "--file1"
            || args[2] != "--format1"
            || args[4] != "--file2"
            || args[6] != "--format2"
        {
            return Err(ArgsError::WrongArg("Incorrect flag".into()));
        }

        let first_format = match args[3].as_str() {
            "bin" => Ok(Format::Bin),
            "csv" => Ok(Format::Csv),
            "txt" => Ok(Format::Txt),
            _ => Err(ArgsError::WrongArg("Incorrect format".into())),
        }?;

        let second_format = match args[7].as_str() {
            "bin" => Ok(Format::Bin),
            "csv" => Ok(Format::Csv),
            "txt" => Ok(Format::Txt),
            _ => Err(ArgsError::WrongArg("Incorrect format".into())),
        }?;

        let first = Compared {
            input: args[1].to_owned(),
            format: first_format,
        };
        let second = Compared {
            input: args[5].to_owned(),
            format: second_format,
        };

        Ok(Self { first, second })
    }
}

fn main() {
    let args = match Args::new() {
        Ok(v) => v,
        Err(e) => {
            println!("Wrong arguments: {}", e);
            return;
        }
    };
    let first = match std::fs::File::open(&args.first.input) {
        Ok(v) => v,
        Err(e) => {
            println!("Failed to open {}: {}", &args.first.input, e);
            return;
        }
    };
    let first_data = match fin_parser::read_from(first, args.first.format) {
        Ok(v) => v,
        Err(e) => {
            println!("Failed to read data from {}: {}", args.first.input, e);
            return;
        }
    };
    let second = match std::fs::File::open(&args.second.input) {
        Ok(v) => v,
        Err(e) => {
            println!("Failed to open {}: {}", &args.second.input, e);
            return;
        }
    };
    let second_data = match fin_parser::read_from(second, args.second.format) {
        Ok(v) => v,
        Err(e) => {
            println!("Failed to read data from {}: {}", args.second.input, e);
            return;
        }
    };
    let is_equal = first_data.iter().zip(second_data.iter()).all(|pair| {
        if pair.0 != pair.1 {
            get_diff(pair);
        }
        pair.0 == pair.1
    });

    if is_equal {
        println!(
            "Records from {} and {} are identical",
            args.first.input, args.second.input
        );
    }
}

fn get_diff(pair: (&Record, &Record)) {
    println!(
        "Record TX_ID: {} and record TX_ID: {} have different values",
        pair.0.get_id(),
        pair.1.get_id()
    );
}
