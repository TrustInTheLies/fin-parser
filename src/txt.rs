use std::io::Read;

use crate::{
    Amount, Body, DescLen, Description, FromUserId, ReadError, Record, Status, Timestamp, ToUserId,
    TxId, TxType, WriteError,
};

const FIELDS: [&str; 9] = [
    "#",
    "TX_ID",
    "TX_TYPE",
    "FROM_USER_ID",
    "TO_USER_ID",
    "AMOUNT",
    "TIMESTAMP",
    "STATUS",
    "DESCRIPTION",
];

#[derive(Debug, Clone)]
pub struct YPBankTxtRecord {
    pub(crate) body: Body,
}

impl YPBankTxtRecord {
    pub(crate) fn parse<R: Read>(mut reader: R) -> Result<Vec<Record>, ReadError> {
        let mut records = Vec::new();
        let mut data = String::new();
        reader
            .read_to_string(&mut data)
            .map_err(|e| ReadError::FailedReader(format!("Failed to read: {}", e)))?;

        let parts = data.trim().split("\n\n").collect::<Vec<&str>>(); //blocks
        parts.iter().try_for_each(|part| {
            for line in part.lines() {
                let mut unsupported_field_exists = true;
                if line.starts_with("#") {
                    continue;
                }
                if !line.contains(":") {
                    return Err(ReadError::IncorrectData("Missing separator".into()));
                }
                for field in FIELDS {
                    if line.starts_with(field) {
                        unsupported_field_exists = false;
                        break;
                    }
                }
                if unsupported_field_exists {
                    return Err(ReadError::IncorrectData("Unsupported field".into()));
                }
            }

            let tx_id = Self::build_field_value(part, "TX_ID")?;
            let tx_id = Self::parse_tx_id(tx_id)?;

            let tx_type = Self::build_field_value(part, "TX_TYPE")?;
            let tx_type = Self::parse_tx_type(tx_type)?;

            let from_user_id = Self::build_field_value(part, "FROM_USER_ID")?;
            let from_user_id = Self::parse_from_user_id(from_user_id, &tx_type)?;

            let to_user_id = Self::build_field_value(part, "TO_USER_ID")?;
            let to_user_id = Self::parse_to_user_id(to_user_id, &tx_type)?;

            let amount = Self::build_field_value(part, "AMOUNT")?;
            let amount = Self::parse_amount(amount)?;

            let timestamp = Self::build_field_value(part, "TIMESTAMP")?;
            let timestamp = Self::parse_timestamp(timestamp)?;

            let status = Self::build_field_value(part, "STATUS")?;
            let status = Self::parse_status(status)?;

            let description = Self::build_field_value(part, "DESCRIPTION")?;
            let description = Self::parse_description(description);

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
            let record = Record::Txt(YPBankTxtRecord { body });
            records.push(record);
            Ok(())
        })?;

        Ok(records)
    }

    fn build_field_value(part: &str, field: &str) -> Result<String, ReadError> {
        let value = part
            .lines()
            .find(|line| line.contains(field))
            .ok_or(ReadError::IncorrectData(format!("Missing {} field", field)))?
            .split(":")
            .last()
            .filter(|v| !v.is_empty())
            .ok_or(ReadError::IncorrectData(format!("Missing {} value", field)))?
            .trim()
            .to_owned();
        Ok(value)
    }

    fn parse_tx_id(value: String) -> Result<TxId, ReadError> {
        let tx_id = value.trim().parse::<u64>()?;
        Ok(TxId(tx_id))
    }

    fn parse_tx_type(value: String) -> Result<TxType, ReadError> {
        match value.trim() {
            "DEPOSIT" => Ok(TxType::Deposit),
            "TRANSFER" => Ok(TxType::Transfer),
            "WITHDRAWAL" => Ok(TxType::Withdrawal),
            _ => Err(ReadError::IncorrectData("Incorrect tx type".into())),
        }
    }

    fn parse_from_user_id(value: String, tx_type: &TxType) -> Result<FromUserId, ReadError> {
        match tx_type {
            TxType::Deposit => Ok(FromUserId(0)),
            _ => Ok(FromUserId(value.trim().parse::<u64>()?)),
        }
    }

    fn parse_to_user_id(value: String, tx_type: &TxType) -> Result<ToUserId, ReadError> {
        match tx_type {
            TxType::Withdrawal => Ok(ToUserId(0)),
            _ => Ok(ToUserId(value.trim().parse::<u64>()?)),
        }
    }

    fn parse_amount(value: String) -> Result<Amount, ReadError> {
        let amount = value.trim().parse::<u64>()?;
        Ok(Amount(amount))
    }

    fn parse_timestamp(value: String) -> Result<Timestamp, ReadError> {
        let timestamp = value.trim().parse::<u64>()?;
        Ok(Timestamp(timestamp))
    }

    fn parse_status(value: String) -> Result<Status, ReadError> {
        match value.trim() {
            "SUCCESS" => Ok(Status::Success),
            "FAILURE" => Ok(Status::Failure),
            "PENDING" => Ok(Status::Pending),
            _ => Err(ReadError::IncorrectData("Incorrect status".into())),
        }
    }

    fn parse_description(value: String) -> Description {
        if value.is_empty() {
            Description(None)
        } else {
            let description = value.as_bytes().to_vec();
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
