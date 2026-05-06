use std::fmt::Display;

use fin_parser::Format;

struct Args {
    input: String,
    input_format: Format,
    output_format: Format,
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
        if args.len() != 6 {
            return Err(ArgsError::WrongCount("Wrong number of arguments".into()));
        }
        if args[0] != "-i" || args[2] != "-if" || args[4] != "-of" {
            return Err(ArgsError::WrongArg("Incorrect flag".into()));
        }

        let input_format = match args[3].as_str() {
            "bin" => Ok(Format::Bin),
            "csv" => Ok(Format::Csv),
            "txt" => Ok(Format::Txt),
            _ => Err(ArgsError::WrongArg("Incorrect format".into())),
        }?;

        let output_format = match args[5].as_str() {
            "bin" => Ok(Format::Bin),
            "csv" => Ok(Format::Csv),
            "txt" => Ok(Format::Txt),
            _ => Err(ArgsError::WrongArg("Incorrect format".into())),
        }?;

        Ok(Self {
            input: args[1].to_owned(),
            input_format,
            output_format,
        })
    }
}

fn main() {
    let args = match Args::new() {
        Ok(v) => v,
        Err(e) => {
            println!("{}", e);
            return;
        }
    };

    let source = match std::fs::File::open(&args.input) {
        Ok(v) => v,
        Err(e) => {
            println!("Failed to open {}: {}", &args.input, e);
            return;
        }
    };
    let data = match fin_parser::read_from(source, args.input_format) {
        Ok(v) => v,
        Err(e) => match e {
            fin_parser::ReadError::FailedReader(v) => {
                println!("{}", v);
                return;
            }
            fin_parser::ReadError::MismatchedSize(v) => {
                println!("{}", v);
                return;
            }
            fin_parser::ReadError::IncorrectData(v) => {
                println!("{}", v);
                return;
            }
        },
    };
    let extension = match args.output_format {
        Format::Bin => "bin",
        Format::Csv => "csv",
        Format::Txt => "txt",
    };

    match fin_parser::write_all_to(&mut std::io::stdout(), data, args.output_format) {
        Ok(_) => println!("Successfully created converted.{} file", extension),
        Err(e) => println!("Failed to write to converted.{}: {}", extension, e),
    };
}
