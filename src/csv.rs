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
        let mut lines = data
            .lines()
            .map(|line| line.trim())
            .filter(|line| !line.is_empty())
            .collect::<Vec<&str>>();
        let header: Vec<&str> = lines.drain(..1).collect();
        if header[0] != CSV_LAYOUT {
            return Err(ReadError::IncorrectData(format!(
                "Incorrect header, expected {}, got {}",
                CSV_LAYOUT, header[0]
            )));
        };
        for line in lines {
            let fields = line.split(",").collect::<Vec<&str>>();
            if fields.len() != REQUIRED_LEN {
                return Err(ReadError::IncorrectData(format!(
                    "Not enough fields, expected {}, got {}",
                    REQUIRED_LEN,
                    fields.len(),
                )));
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
        let field = field.parse::<u64>()?;
        match tx_type {
            TxType::Deposit => Ok(FromUserId(0)),
            _ => Ok(FromUserId(field)),
        }
    }

    fn parse_to_user_id(field: &str, tx_type: &TxType) -> Result<ToUserId, ReadError> {
        let field = field.parse::<u64>()?;
        match tx_type {
            TxType::Withdrawal => Ok(ToUserId(0)),
            _ => Ok(ToUserId(field)),
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

mod tests {
    #[cfg(test)]
    use crate::{
        Amount, Body, DescLen, Description, FromUserId, Status, Timestamp, ToUserId, TxId, TxType,
        csv::YPBankCsvRecord,
    };

    #[cfg(test)]
    fn create_body_with_desc(txtype: TxType, status: Status, size: Option<u32>) -> Body {
        Body(
            TxId(1),
            txtype,
            FromUserId(0),
            ToUserId(1),
            Amount(1),
            Timestamp(1),
            status,
            DescLen(size),
            Description(Some(vec![1, 2, 3])),
        )
    }

    #[cfg(test)]
    fn create_body_without_desc(txtype: TxType, status: Status) -> Body {
        Body(
            TxId(1),
            txtype,
            FromUserId(0),
            ToUserId(1),
            Amount(1),
            Timestamp(1),
            status,
            DescLen(None),
            Description(None),
        )
    }

    #[cfg(test)]
    fn create_csv_record(
        txtype: TxType,
        status: Status,
        size: Option<u32>,
        with_desc: bool,
    ) -> YPBankCsvRecord {
        YPBankCsvRecord {
            body: if with_desc {
                create_body_with_desc(txtype, status, size)
            } else {
                create_body_without_desc(txtype, status)
            },
        }
    }

    #[test]
    fn test_write_description_with_data() {
        let record = create_csv_record(TxType::Deposit, Status::Failure, None, true);
        assert!(record.write_description().is_ok());
    }

    #[test]
    fn test_write_description_without_data() {
        let record = create_csv_record(TxType::Deposit, Status::Failure, None, false);
        assert!(record.write_description().is_ok_and(|v| v.is_empty()));
    }

    #[test]
    fn test_write_description_broken_utf8() {
        let mut record = create_csv_record(TxType::Deposit, Status::Failure, None, true);
        record.body.8.0 = Some([0xFF, 0xFF].as_slice().to_vec());
        assert!(record.write_description().is_err());
    }

    #[test]
    fn test_parse_description_empty() {
        assert!(YPBankCsvRecord::parse_description("").0.is_none());
    }

    #[test]
    fn test_parse_description_with_data() {
        assert!(
            YPBankCsvRecord::parse_description("oleg")
                .0
                .is_some_and(|v| v == "oleg".as_bytes().to_vec())
        );
    }

    #[test]
    fn test_parse_status_success() {
        assert!(YPBankCsvRecord::parse_status("SUCCESS").is_ok_and(|v| v == Status::Success));
        assert!(YPBankCsvRecord::parse_status("FAILURE").is_ok_and(|v| v == Status::Failure));
        assert!(YPBankCsvRecord::parse_status("PENDING").is_ok_and(|v| v == Status::Pending));
    }

    #[test]
    fn test_parse_status_failed() {
        assert!(YPBankCsvRecord::parse_status("oleg").is_err());
    }

    #[test]
    fn test_parse_timestamp_failed() {
        assert!(YPBankCsvRecord::parse_timestamp("-9").is_err());
        assert!(YPBankCsvRecord::parse_timestamp("oleg").is_err());
    }

    #[test]
    fn test_parse_timestamp_success() {
        assert!(YPBankCsvRecord::parse_timestamp("24").is_ok());
    }

    #[test]
    fn test_parse_amount_failed() {
        assert!(YPBankCsvRecord::parse_amount("-9").is_err());
        assert!(YPBankCsvRecord::parse_amount("oleg").is_err());
    }

    #[test]
    fn test_parse_amount_success() {
        assert!(YPBankCsvRecord::parse_amount("24").is_ok());
    }

    #[test]
    fn test_parse_from_user_id_failed() {
        let tx_type = TxType::Deposit;
        assert!(YPBankCsvRecord::parse_from_user_id("field", &tx_type).is_err());
        assert!(YPBankCsvRecord::parse_from_user_id("22.22", &tx_type).is_err());
    }

    #[test]
    fn test_parse_from_user_id_success() {
        let tx_type = TxType::Deposit;
        assert!(YPBankCsvRecord::parse_from_user_id("11", &tx_type).is_ok_and(|v| v.0 == 0));
        let tx_type = TxType::Transfer;
        assert!(YPBankCsvRecord::parse_from_user_id("11", &tx_type).is_ok_and(|v| v.0 == 11));
        let tx_type = TxType::Withdrawal;
        assert!(YPBankCsvRecord::parse_from_user_id("11", &tx_type).is_ok_and(|v| v.0 == 11));
    }

    #[test]
    fn test_parse_to_user_id_failed() {
        let tx_type = TxType::Deposit;
        assert!(YPBankCsvRecord::parse_to_user_id("field", &tx_type).is_err());
        assert!(YPBankCsvRecord::parse_to_user_id("22.22", &tx_type).is_err());
    }

    #[test]
    fn test_parse_to_user_id_success() {
        let tx_type = TxType::Withdrawal;
        assert!(YPBankCsvRecord::parse_to_user_id("11", &tx_type).is_ok_and(|v| v.0 == 0));
        let tx_type = TxType::Transfer;
        assert!(YPBankCsvRecord::parse_to_user_id("11", &tx_type).is_ok_and(|v| v.0 == 11));
        let tx_type = TxType::Deposit;
        assert!(YPBankCsvRecord::parse_to_user_id("11", &tx_type).is_ok_and(|v| v.0 == 11));
    }

    #[test]
    fn test_parse_tx_id_success() {
        assert!(YPBankCsvRecord::parse_tx_id("11").is_ok());
    }

    #[test]
    fn test_parse_tx_id_failed() {
        assert!(YPBankCsvRecord::parse_tx_id("-11").is_err());
        assert!(YPBankCsvRecord::parse_tx_id("22.1").is_err());
        assert!(YPBankCsvRecord::parse_tx_id("oleg").is_err());
    }

    #[test]
    fn test_parse_tx_type_success() {
        assert!(matches!(
            YPBankCsvRecord::parse_tx_type("DEPOSIT"),
            Ok(TxType::Deposit)
        ));
        assert!(matches!(
            YPBankCsvRecord::parse_tx_type("TRANSFER"),
            Ok(TxType::Transfer)
        ));
        assert!(matches!(
            YPBankCsvRecord::parse_tx_type("WITHDRAWAL"),
            Ok(TxType::Withdrawal)
        ));
    }

    #[test]
    fn test_parse_tx_type_failed() {
        assert!(YPBankCsvRecord::parse_tx_type("oleg").is_err());
    }

    #[test]
    fn test_parse_empty_line() {
        let text = "
            TX_ID,TX_TYPE,FROM_USER_ID,TO_USER_ID,AMOUNT,TIMESTAMP,STATUS,DESCRIPTION

            1000000000000000,DEPOSIT,0,9223372036854775807,100,1633036860000,FAILURE,\"Record number 1\"


            1000000000000001,TRANSFER,9223372036854775807,9223372036854775807,200,1633036920000,PENDING,\"Record number 2\"
            ";
        let reader = text.as_bytes();
        let result = YPBankCsvRecord::parse(reader);
        assert!(result.is_ok_and(|v| v.len() == 2));
    }

    #[test]
    fn test_parse_failed_header() {
        let text = "TX_ID,TX_TYPE,FROM_USER_ID,TO_USER_ID,OO,TIMESTAMP,STATUS,DESCRIPTION
        1000000000000000,DEPOSIT,0,9223372036854775807,100,1633036860000,FAILURE,\"Record number 1\"
        1000000000000001,TRANSFER,9223372036854775807,9223372036854775807,200,1633036920000,PENDING,\"Record number 2\"";
        let reader = text.as_bytes();
        let result = YPBankCsvRecord::parse(reader);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_failed_len() {
        let text = "TX_ID,TX_TYPE,FROM_USER_ID,TO_USER_ID,OO,TIMESTAMP,STATUS,DESCRIPTION
        1000000000000000,DEPOSIT,0,100,1633036860000,FAILURE,\"Record number 1\"
        1000000000000001,TRANSFER,9223372036854775807,9223372036854775807,200,1633036920000,PENDING,\"Record number 2\"";
        let reader = text.as_bytes();
        let result = YPBankCsvRecord::parse(reader);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_success() {
        let text = "TX_ID,TX_TYPE,FROM_USER_ID,TO_USER_ID,AMOUNT,TIMESTAMP,STATUS,DESCRIPTION
            1000000000000000,DEPOSIT,0,9223372036854775807,100,1633036860000,FAILURE,\"Record number 1\"
            1000000000000001,TRANSFER,9223372036854775807,9223372036854775807,200,1633036920000,PENDING,\"Record number 2\"";
        let reader = text.as_bytes();
        let result = YPBankCsvRecord::parse(reader);
        assert!(result.is_ok_and(|v| v[0].get_id() == 1000000000000000));
    }
}
