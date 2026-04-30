use std::io::Read;

use crate::{
    Amount, Body, DescLen, Description, FromUserId, ReadError, Record, Status, Timestamp, ToUserId,
    TxId, TxType, WriteError,
};

pub(crate) const CSV_LAYOUT: &str =
    "TX_ID,TX_TYPE,FROM_USER_ID,TO_USER_ID,AMOUNT,TIMESTAMP,STATUS,DESCRIPTION";
const REQUIRED_LEN: usize = 8;

#[derive(Debug, Clone)]
pub struct YPBankCsvRecord {
    pub(crate) body: Body,
}

impl YPBankCsvRecord {
    pub(crate) fn parse<R: Read>(mut reader: R) -> Result<Vec<Record>, ReadError> {
        let mut records = Vec::new();
        let mut data = String::new();
        reader
            .read_to_string(&mut data)
            .map_err(|e| ReadError::FailedReader(format!("Failed to read: {}", e)))?;
        let mut lines = data.lines().collect::<Vec<&str>>();
        let header: Vec<&str> = lines.drain(..1).collect();
        if header[0] != CSV_LAYOUT {
            return Err(ReadError::IncorrectData("Incorrect header".into()));
        };
        for line in lines {
            if line.is_empty() {
                continue;
            }
            let fields = line.split(",").collect::<Vec<&str>>();
            if fields.len() != REQUIRED_LEN {
                return Err(ReadError::IncorrectData("Not enough fields".into()));
            }
            let tx_id = Self::parse_tx_id(fields[0])?;
            let tx_type = Self::parse_tx_type(fields[1])?;
            let from_user_id = Self::parse_from_user_id(fields[2], &tx_type)?;
            let to_user_id = Self::parse_to_user_id(fields[3], &tx_type)?;
            let amount = Self::parse_amount(fields[4])?;
            let timestamp = Self::parse_timestamp(fields[5])?;
            let status = Self::parse_status(fields[6])?;
            let description = Self::parse_description(fields[7]);
            let body = Body(
                tx_id,
                tx_type,
                from_user_id,
                to_user_id,
                amount,
                timestamp,
                status,
                DescLen(None),
                description,
            );
            let record = Record::Csv(YPBankCsvRecord { body });
            records.push(record);
        }
        Ok(records)
    }

    fn parse_tx_id(field: &str) -> Result<TxId, ReadError> {
        let tx_id = field.parse::<u64>()?;
        Ok(TxId(tx_id))
    }

    fn parse_tx_type(field: &str) -> Result<TxType, ReadError> {
        match field {
            "DEPOSIT" => Ok(TxType::Deposit),
            "TRANSFER" => Ok(TxType::Transfer),
            "WITHDRAWAL" => Ok(TxType::Withdrawal),
            _ => Err(ReadError::IncorrectData(format!(
                "Incorrect tx type: {}",
                field
            ))),
        }
    }

    fn parse_from_user_id(field: &str, tx_type: &TxType) -> Result<FromUserId, ReadError> {
        match tx_type {
            TxType::Deposit => Ok(FromUserId(0)),
            _ => Ok(FromUserId(field.parse::<u64>()?)),
        }
    }

    fn parse_to_user_id(field: &str, tx_type: &TxType) -> Result<ToUserId, ReadError> {
        match tx_type {
            TxType::Withdrawal => Ok(ToUserId(0)),
            _ => Ok(ToUserId(field.parse::<u64>()?)),
        }
    }

    fn parse_amount(field: &str) -> Result<Amount, ReadError> {
        let amount = field.parse::<u64>()?;
        Ok(Amount(amount))
    }

    fn parse_timestamp(field: &str) -> Result<Timestamp, ReadError> {
        let timestamp = field.parse::<u64>()?;
        Ok(Timestamp(timestamp))
    }

    fn parse_status(field: &str) -> Result<Status, ReadError> {
        match field {
            "SUCCESS" => Ok(Status::Success),
            "FAILURE" => Ok(Status::Failure),
            "PENDING" => Ok(Status::Pending),
            _ => Err(ReadError::IncorrectData("Incorrect status".into())),
        }
    }

    fn parse_description(field: &str) -> Description {
        if field.is_empty() {
            Description(None)
        } else {
            let description = field.as_bytes().to_vec();
            Description(Some(description))
        }
    }

    pub(crate) fn write_description(&self) -> Result<String, WriteError> {
        let description = match &self.body.8.0 {
            Some(v) => String::from_utf8(v.to_owned())?.trim().to_owned(),
            None => "".into(),
        };
        Ok(description)
    }
}
