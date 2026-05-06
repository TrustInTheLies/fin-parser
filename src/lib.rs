use std::{
    array::TryFromSliceError,
    fmt::Display,
    io::{Read, Write},
    num::ParseIntError,
    string::FromUtf8Error,
};

use crate::{
    bin::{MAGIC, SIZE_WITHOUT_DESCRIPTION, YPBankBinRecord},
    csv::{CSV_LAYOUT, YPBankCsvRecord},
    txt::YPBankTxtRecord,
    write::{write_bin, write_csv, write_txt},
};

mod bin;
mod csv;
mod txt;
mod write;

#[derive(Debug, Clone, Copy)]
enum TxType {
    Deposit,
    Transfer,
    Withdrawal,
}

#[derive(Debug, Clone, Copy)]
enum Status {
    Success,
    Failure,
    Pending,
}

/// Represents one of internal types used for storing data from different file types.
/// Returned from [`read_from`]
#[derive(Debug)]
pub enum Record {
    Bin(YPBankBinRecord),
    Csv(YPBankCsvRecord),
    Txt(YPBankTxtRecord),
}

/// Represents input/output format for files.
/// Used in [`read_from`]and [`write_all_to`]
pub enum Format {
    Bin,
    Csv,
    Txt,
}

/// Error returned from [`read_from`].
#[derive(Debug)]
pub enum ReadError {
    /// Failed to get access to reader, occurs either because of wrong path or because of missing permissions
    FailedReader(String),
    /// Failed to parse binary data, occurs either because of incorrect field size or because of overflowing integers
    MismatchedSize(String),
    /// Failed to parse fields, occurs when provided files violate format schema
    IncorrectData(String),
}

impl Display for ReadError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ReadError::FailedReader(v) => write!(f, "{}", v),
            ReadError::MismatchedSize(v) => write!(f, "{}", v),
            ReadError::IncorrectData(v) => write!(f, "{}", v),
        }
    }
}

/// Error returned from [`write_all_to`].
#[derive(Debug)]
pub enum WriteError {
    /// Failed to get access to writer, occurs either because of wrong path or because of missing permissions
    FailedWriter(String),
    /// Failed to write contents to .csv or .txt files, occurs when provided data contains invalid UTF-8
    InvalidEncoding(String),
    /// Provided record satisfies schema format but contains incorrect values, i.e. FROM_USER_ID != 0 when TX_TYPE is DEPOSIT
    IncorrectData(String),
}

impl Display for WriteError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WriteError::FailedWriter(v) => write!(f, "{}", v),
            WriteError::InvalidEncoding(v) => write!(f, "{}", v),
            WriteError::IncorrectData(v) => write!(f, "{}", v),
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct TxId(u64);
#[derive(Debug, Clone, Copy)]
struct FromUserId(u64);
#[derive(Debug, Clone, Copy)]
struct ToUserId(u64);
#[derive(Debug, Clone, Copy)]
struct Amount(u64);
#[derive(Debug, Clone, Copy)]
struct Timestamp(u64);
#[derive(Debug, Clone, Copy)]
struct DescLen(Option<u32>);
#[derive(Debug, Clone)]
struct Description(Option<Vec<u8>>);

#[derive(Debug, Clone, Copy)]
struct Head {
    magic: u32,
    record_size: u32,
}

#[derive(Debug, Clone)]
struct Body(
    TxId,
    TxType,
    FromUserId,
    ToUserId,
    Amount,
    Timestamp,
    Status,
    DescLen,
    Description,
);

impl From<TryFromSliceError> for ReadError {
    fn from(value: TryFromSliceError) -> Self {
        Self::MismatchedSize(value.to_string())
    }
}

impl From<ParseIntError> for ReadError {
    fn from(value: ParseIntError) -> Self {
        match value.kind() {
            std::num::IntErrorKind::PosOverflow => Self::MismatchedSize("Integer overflow".into()),
            std::num::IntErrorKind::NegOverflow => Self::MismatchedSize("Integer overflow".into()),
            _ => Self::IncorrectData("Not a number".into()),
        }
    }
}

impl From<FromUtf8Error> for WriteError {
    fn from(value: FromUtf8Error) -> Self {
        Self::InvalidEncoding(value.to_string())
    }
}

impl Display for TxType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TxType::Deposit => write!(f, "DEPOSIT"),
            TxType::Transfer => write!(f, "TRANSFER"),
            TxType::Withdrawal => write!(f, "WITHDRAWAL"),
        }
    }
}

impl Display for Status {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Status::Success => write!(f, "SUCCESS"),
            Status::Failure => write!(f, "FAILURE"),
            Status::Pending => write!(f, "PENDING"),
        }
    }
}

impl From<&YPBankBinRecord> for YPBankCsvRecord {
    fn from(value: &YPBankBinRecord) -> Self {
        Self {
            body: Body(
                value.body.0,
                value.body.1,
                value.body.2,
                value.body.3,
                value.body.4,
                value.body.5,
                value.body.6,
                value.body.7,
                value.body.8.clone(),
            ),
        }
    }
}

impl From<&YPBankTxtRecord> for YPBankCsvRecord {
    fn from(value: &YPBankTxtRecord) -> Self {
        Self {
            body: Body(
                value.body.0,
                value.body.1,
                value.body.2,
                value.body.3,
                value.body.4,
                value.body.5,
                value.body.6,
                value.body.7,
                value.body.8.clone(),
            ),
        }
    }
}

impl From<&YPBankBinRecord> for YPBankTxtRecord {
    fn from(value: &YPBankBinRecord) -> Self {
        Self {
            body: Body(
                value.body.0,
                value.body.1,
                value.body.2,
                value.body.3,
                value.body.4,
                value.body.5,
                value.body.6,
                value.body.7,
                value.body.8.clone(),
            ),
        }
    }
}

impl From<&YPBankCsvRecord> for YPBankTxtRecord {
    fn from(value: &YPBankCsvRecord) -> Self {
        Self {
            body: Body(
                value.body.0,
                value.body.1,
                value.body.2,
                value.body.3,
                value.body.4,
                value.body.5,
                value.body.6,
                value.body.7,
                value.body.8.clone(),
            ),
        }
    }
}

impl From<&YPBankCsvRecord> for YPBankBinRecord {
    fn from(value: &YPBankCsvRecord) -> Self {
        let desc_len = if let Some(desc) = &value.body.8.0 {
            desc.len() as u32
        } else {
            0
        };
        let record_size = SIZE_WITHOUT_DESCRIPTION + desc_len;
        let head = Head {
            magic: u32::from_be_bytes(MAGIC),
            record_size,
        };
        Self {
            head,
            body: Body(
                value.body.0,
                value.body.1,
                value.body.2,
                value.body.3,
                value.body.4,
                value.body.5,
                value.body.6,
                DescLen(Some(desc_len)),
                value.body.8.clone(),
            ),
        }
    }
}

impl From<&YPBankTxtRecord> for YPBankBinRecord {
    fn from(value: &YPBankTxtRecord) -> Self {
        let desc_len = if let Some(desc) = &value.body.8.0 {
            desc.len() as u32
        } else {
            0
        };
        let record_size = SIZE_WITHOUT_DESCRIPTION + desc_len;
        let head = Head {
            magic: u32::from_be_bytes(MAGIC),
            record_size,
        };
        Self {
            head,
            body: Body(
                value.body.0,
                value.body.1,
                value.body.2,
                value.body.3,
                value.body.4,
                value.body.5,
                value.body.6,
                DescLen(Some(desc_len)),
                value.body.8.clone(),
            ),
        }
    }
}

impl PartialEq for TxType {
    fn eq(&self, other: &Self) -> bool {
        core::mem::discriminant(self) == core::mem::discriminant(other)
    }
}

impl PartialEq for Status {
    fn eq(&self, other: &Self) -> bool {
        core::mem::discriminant(self) == core::mem::discriminant(other)
    }
}

impl PartialEq for Body {
    fn eq(&self, other: &Self) -> bool {
        self.0.0 == other.0.0
            && self.1 == other.1
            && self.2.0 == other.2.0
            && self.3.0 == other.3.0
            && self.4.0 == other.4.0
            && self.5.0 == other.5.0
            && self.6 == other.6
            && self.8.0 == other.8.0
    }
}

impl PartialEq for Record {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Record::Bin(l), Record::Bin(r)) => l.body == r.body,
            (Record::Bin(l), Record::Csv(r)) => l.body == r.body,
            (Record::Bin(l), Record::Txt(r)) => l.body == r.body,
            (Record::Csv(l), Record::Bin(r)) => l.body == r.body,
            (Record::Csv(l), Record::Csv(r)) => l.body == r.body,
            (Record::Csv(l), Record::Txt(r)) => l.body == r.body,
            (Record::Txt(l), Record::Bin(r)) => l.body == r.body,
            (Record::Txt(l), Record::Csv(r)) => l.body == r.body,
            (Record::Txt(l), Record::Txt(r)) => l.body == r.body,
        }
    }
}

impl Record {
    fn write_to<W: Write>(self, writer: &mut W, format: &Format) -> Result<(), WriteError> {
        match self {
            Record::Bin(_) => match format {
                Format::Bin => write_bin(&self, writer),
                Format::Csv => write_csv(&self, writer),
                Format::Txt => write_txt(&self, writer),
            },
            Record::Csv(_) => match format {
                Format::Bin => write_bin(&self, writer),
                Format::Csv => write_csv(&self, writer),
                Format::Txt => write_txt(&self, writer),
            },
            Record::Txt(_) => match format {
                Format::Bin => write_bin(&self, writer),
                Format::Csv => write_csv(&self, writer),
                Format::Txt => write_txt(&self, writer),
            },
        }
    }

    /// Returns TX_ID value from record.
    pub fn get_id(&self) -> u64 {
        match self {
            Record::Bin(ypbank_bin_record) => ypbank_bin_record.body.0.0,
            Record::Csv(ypbank_csv_record) => ypbank_csv_record.body.0.0,
            Record::Txt(ypbank_txt_record) => ypbank_txt_record.body.0.0,
        }
    }
}

/// Writes a new entry to `writer` based on provided [`data`](crate::Record) in required [`format`](crate::Format)
pub fn write_all_to<W: Write>(
    writer: &mut W,
    data: Vec<Record>,
    format: Format,
) -> Result<(), WriteError> {
    match format {
        Format::Csv => {
            writer
                .write_all(format!("{}\n", CSV_LAYOUT).as_bytes())
                .map_err(|e| WriteError::FailedWriter(format!("Failed to write: {}", e)))?;
            for item in data {
                item.write_to(writer, &format)?;
            }
        }
        _ => {
            for item in data {
                item.write_to(writer, &format)?;
            }
        }
    }
    Ok(())
}

/// Reads contents from `reader`, requires [`format`](crate::Format), returns [`data`](crate::Record) which can be used for [`writing`](crate::write_all_to)
pub fn read_from<R: Read>(reader: R, format: Format) -> Result<Vec<Record>, ReadError> {
    match format {
        Format::Bin => YPBankBinRecord::parse(reader),
        Format::Csv => YPBankCsvRecord::parse(reader),
        Format::Txt => YPBankTxtRecord::parse(reader),
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        Amount, Body, DescLen, Description, Format, FromUserId, Head, ReadError, Record, Status,
        Timestamp, ToUserId, TxId, TxType, WriteError,
        bin::{MAGIC, SIZE_WITHOUT_DESCRIPTION, YPBankBinRecord},
        csv::YPBankCsvRecord,
        read_from,
        txt::YPBankTxtRecord,
        write_all_to,
    };

    fn prep_csv_output(data: &str) -> Result<Vec<Record>, ReadError> {
        let reader = data.as_bytes();
        read_from(reader, crate::Format::Csv)
    }

    #[test]
    fn test_read_from_csv_success() {
        let output = prep_csv_output(
            "TX_ID,TX_TYPE,FROM_USER_ID,TO_USER_ID,AMOUNT,TIMESTAMP,STATUS,DESCRIPTION\n1000000000000000,DEPOSIT,0,9223372036854775807,100,1633036860000,FAILURE,\"Record number 1\"",
        );
        assert!(output.is_ok());
    }

    #[test]
    fn test_read_from_csv_failed() {
        // removed tx_type from header and timestamp value from row
        let output = prep_csv_output(
            "TX_ID,FROM_USER_ID,TO_USER_ID,AMOUNT,TIMESTAMP,STATUS,DESCRIPTION\n1000000000000000,DEPOSIT,0,9223372036854775807,100,FAILURE,\"Record number 1\"",
        );
        assert!(output.is_err());
    }

    #[test]
    fn test_read_from_csv_fields() {
        let output = prep_csv_output(
            "TX_ID,TX_TYPE,FROM_USER_ID,TO_USER_ID,AMOUNT,TIMESTAMP,STATUS,DESCRIPTION\n1000000000000000,DEPOSIT,0,9223372036854775807,100,1633036860000,FAILURE,\"Record number 1\"",
        ).unwrap();
        match output[0] {
            crate::Record::Csv(ref ypbank_csv_record) => {
                assert_eq!(ypbank_csv_record.body.0.0, 1000000000000000);
                assert_eq!(ypbank_csv_record.body.1, TxType::Deposit);
                assert_eq!(ypbank_csv_record.body.2.0, 0);
                assert_eq!(ypbank_csv_record.body.3.0, 9223372036854775807);
                assert_eq!(ypbank_csv_record.body.4.0, 100);
                assert_eq!(ypbank_csv_record.body.5.0, 1633036860000);
                assert_eq!(ypbank_csv_record.body.6, Status::Failure);
                assert_eq!(ypbank_csv_record.body.7.0, None);
                assert_eq!(
                    ypbank_csv_record.body.8.0,
                    Some("\"Record number 1\"".as_bytes().to_vec())
                );
            }
            _ => panic!("Unexpected Record type"),
        }
    }

    fn prep_txt_output(data: &str) -> Result<Vec<Record>, ReadError> {
        let reader = data.as_bytes();
        read_from(reader, crate::Format::Txt)
    }

    #[test]
    fn test_read_from_txt_success() {
        let output = prep_txt_output(
            "# Record 1 (DEPOSIT)\nTX_TYPE: DEPOSIT\nTO_USER_ID: 9223372036854775807\nFROM_USER_ID: 0\nTIMESTAMP: 1633036860000\nDESCRIPTION: \"Record number 1\"\nTX_ID: 1000000000000000\nAMOUNT: 100\nSTATUS: FAILURE",
        );
        assert!(output.is_ok());
    }

    #[test]
    fn test_read_from_txt_failed() {
        // added typo to AMOUNT
        let output = prep_txt_output(
            "# Record 1 (DEPOSIT)\nTX_TYPE: DEPOSIT\nTO_USER_ID: 9223372036854775807\nFROM_USER_ID: 0\nTIMESTAMP: 1633036860000\nDESCRIPTION: \"Record number 1\"\nTX_ID: 1000000000000000\nMOUNT: 100\nSTATUS: FAILURE",
        );
        assert!(output.is_err());
    }

    #[test]
    fn test_read_from_txt_fields() {
        let output = prep_txt_output(
            "# Record 1 (DEPOSIT)\nTX_TYPE: DEPOSIT\nTO_USER_ID: 9223372036854775807\nFROM_USER_ID: 0\nTIMESTAMP: 1633036860000\nDESCRIPTION: \"Record number 1\"\nTX_ID: 1000000000000000\nAMOUNT: 100\nSTATUS: FAILURE",
        ).unwrap();
        match output[0] {
            crate::Record::Txt(ref ypbank_txt_record) => {
                assert_eq!(ypbank_txt_record.body.0.0, 1000000000000000);
                assert_eq!(ypbank_txt_record.body.1, TxType::Deposit);
                assert_eq!(ypbank_txt_record.body.2.0, 0);
                assert_eq!(ypbank_txt_record.body.3.0, 9223372036854775807);
                assert_eq!(ypbank_txt_record.body.4.0, 100);
                assert_eq!(ypbank_txt_record.body.5.0, 1633036860000);
                assert_eq!(ypbank_txt_record.body.6, Status::Failure);
                assert_eq!(ypbank_txt_record.body.7.0, None);
                assert_eq!(
                    ypbank_txt_record.body.8.0,
                    Some("\"Record number 1\"".as_bytes().to_vec())
                );
            }
            _ => panic!("Unexpected Record type"),
        }
    }

    fn prep_bin_output(data: &[u8]) -> Result<Vec<Record>, ReadError> {
        read_from(data, crate::Format::Bin)
    }

    #[test]
    fn test_read_from_bin_success() {
        // copied bytes for first two entries from records_example.bin
        let output = prep_bin_output(&[
            89, 80, 66, 78, 0, 0, 0, 63, 0, 3, 141, 126, 164, 198, 128, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 127, 255, 255, 255, 255, 255, 255, 255, 0, 0, 0, 0, 0, 0, 0, 100, 0, 0, 1, 124, 56,
            148, 250, 96, 1, 0, 0, 0, 17, 34, 82, 101, 99, 111, 114, 100, 32, 110, 117, 109, 98,
            101, 114, 32, 49, 34, 89, 80, 66, 78, 0, 0, 0, 63, 0, 3, 141, 126, 164, 198, 128, 1, 1,
            127, 255, 255, 255, 255, 255, 255, 255, 127, 255, 255, 255, 255, 255, 255, 255, 0, 0,
            0, 0, 0, 0, 0, 200, 0, 0, 1, 124, 56, 149, 228, 192, 2, 0, 0, 0, 17, 34, 82, 101, 99,
            111, 114, 100, 32, 110, 117, 109, 98, 101, 114, 32, 50, 34,
        ]);
        assert!(output.is_ok());
    }

    #[test]
    fn test_read_from_bin_failed() {
        // removed first byte
        let output = prep_bin_output(&[
            80, 66, 78, 0, 0, 0, 63, 0, 3, 141, 126, 164, 198, 128, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            127, 255, 255, 255, 255, 255, 255, 255, 0, 0, 0, 0, 0, 0, 0, 100, 0, 0, 1, 124, 56,
            148, 250, 96, 1, 0, 0, 0, 17, 34, 82, 101, 99, 111, 114, 100, 32, 110, 117, 109, 98,
            101, 114, 32, 49, 34, 89, 80, 66, 78, 0, 0, 0, 63, 0, 3, 141, 126, 164, 198, 128, 1, 1,
            127, 255, 255, 255, 255, 255, 255, 255, 127, 255, 255, 255, 255, 255, 255, 255, 0, 0,
            0, 0, 0, 0, 0, 200, 0, 0, 1, 124, 56, 149, 228, 192, 2, 0, 0, 0, 17, 34, 82, 101, 99,
            111, 114, 100, 32, 110, 117, 109, 98, 101, 114, 32, 50, 34,
        ]);
        assert!(output.is_err());
    }

    #[test]
    fn test_read_from_bin_fields() {
        let output = prep_bin_output(&[
            89, 80, 66, 78, 0, 0, 0, 63, 0, 3, 141, 126, 164, 198, 128, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 127, 255, 255, 255, 255, 255, 255, 255, 0, 0, 0, 0, 0, 0, 0, 100, 0, 0, 1, 124, 56,
            148, 250, 96, 1, 0, 0, 0, 17, 34, 82, 101, 99, 111, 114, 100, 32, 110, 117, 109, 98,
            101, 114, 32, 49, 34, 89, 80, 66, 78, 0, 0, 0, 63, 0, 3, 141, 126, 164, 198, 128, 1, 1,
            127, 255, 255, 255, 255, 255, 255, 255, 127, 255, 255, 255, 255, 255, 255, 255, 0, 0,
            0, 0, 0, 0, 0, 200, 0, 0, 1, 124, 56, 149, 228, 192, 2, 0, 0, 0, 17, 34, 82, 101, 99,
            111, 114, 100, 32, 110, 117, 109, 98, 101, 114, 32, 50, 34,
        ])
        .unwrap();
        match output[0] {
            crate::Record::Bin(ref ypbank_bin_record) => {
                assert_eq!(ypbank_bin_record.body.0.0, 1000000000000000);
                assert_eq!(ypbank_bin_record.body.1, TxType::Deposit);
                assert_eq!(ypbank_bin_record.body.2.0, 0);
                assert_eq!(ypbank_bin_record.body.3.0, 9223372036854775807);
                assert_eq!(ypbank_bin_record.body.4.0, 100);
                assert_eq!(ypbank_bin_record.body.5.0, 1633036860000);
                assert_eq!(ypbank_bin_record.body.6, Status::Failure);
                assert_eq!(
                    ypbank_bin_record.body.7.0,
                    Some(ypbank_bin_record.body.8.0.clone().unwrap().len() as u32)
                );
                assert_eq!(
                    ypbank_bin_record.body.8.0,
                    Some("\"Record number 1\"".as_bytes().to_vec())
                );
            }
            _ => panic!("Unexpected Record type"),
        }
    }

    fn write(data: Vec<Record>, format: Format) -> Result<(), WriteError> {
        let mut buffer = Vec::new();
        write_all_to(&mut buffer, data, format)
    }

    fn create_correct_csv_record() -> Record {
        Record::Csv(YPBankCsvRecord {
            body: Body(
                TxId(1),
                TxType::Deposit,
                FromUserId(0),
                ToUserId(1),
                Amount(1),
                Timestamp(1),
                Status::Failure,
                DescLen(None),
                Description(Some(Vec::from("hello"))),
            ),
        })
    }

    fn create_incorrect_txt_record() -> Record {
        // DescLen не используется в txt
        Record::Txt(YPBankTxtRecord {
            body: Body(
                TxId(1),
                TxType::Deposit,
                FromUserId(0),
                ToUserId(1),
                Amount(1),
                Timestamp(1),
                Status::Failure,
                DescLen(Some(4)),
                Description(Some(Vec::from("bye"))),
            ),
        })
    }

    fn create_incorrect_bin_record() -> Record {
        let head = Head {
            magic: 42,
            record_size: 42,
        };
        Record::Bin(YPBankBinRecord {
            head,
            body: Body(
                TxId(1),
                TxType::Deposit,
                FromUserId(0),
                ToUserId(1),
                Amount(1),
                Timestamp(1),
                Status::Failure,
                DescLen(Some(4)),
                Description(Some(Vec::from("bye"))),
            ),
        })
    }

    fn create_correct_bin_record() -> Record {
        // 3 cause description = bye
        let record_size = SIZE_WITHOUT_DESCRIPTION + 3;
        let head = Head {
            magic: u32::from_be_bytes(MAGIC),
            record_size,
        };
        Record::Bin(YPBankBinRecord {
            head,
            body: Body(
                TxId(1),
                TxType::Deposit,
                FromUserId(0),
                ToUserId(1),
                Amount(1),
                Timestamp(1),
                Status::Failure,
                DescLen(Some(3)),
                Description(Some(Vec::from("bye"))),
            ),
        })
    }

    #[test]
    fn test_write_csv_success() {
        let data = vec![create_correct_csv_record()];
        let result = write(data, Format::Csv);
        assert!(result.is_ok());
    }

    #[test]
    fn test_write_txt_failed() {
        let data = vec![create_incorrect_txt_record()];
        let result = write(data, Format::Csv);
        assert!(result.is_err());
    }

    #[test]
    fn test_write_bin_failed() {
        let data = vec![create_incorrect_bin_record()];
        let result = write(data, Format::Bin);
        assert!(result.is_err())
    }

    #[test]
    fn test_write_bin_success() {
        let data = vec![create_correct_bin_record()];
        let result = write(data, Format::Bin);
        assert!(result.is_ok())
    }
}
