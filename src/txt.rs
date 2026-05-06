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
                let line = line.trim();
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
            let tx_id = Self::parse_tx_id(&tx_id)?;

            let tx_type = Self::build_field_value(part, "TX_TYPE")?;
            let tx_type = Self::parse_tx_type(&tx_type)?;

            let from_user_id = Self::build_field_value(part, "FROM_USER_ID")?;
            let from_user_id = Self::parse_from_user_id(&from_user_id, &tx_type)?;

            let to_user_id = Self::build_field_value(part, "TO_USER_ID")?;
            let to_user_id = Self::parse_to_user_id(&to_user_id, &tx_type)?;

            let amount = Self::build_field_value(part, "AMOUNT")?;
            let amount = Self::parse_amount(&amount)?;

            let timestamp = Self::build_field_value(part, "TIMESTAMP")?;
            let timestamp = Self::parse_timestamp(&timestamp)?;

            let status = Self::build_field_value(part, "STATUS")?;
            let status = Self::parse_status(&status)?;

            let description = Self::build_field_value(part, "DESCRIPTION")?;
            let description = Self::parse_description(&description);

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
            .map(|part| part.trim())
            .filter(|v| !v.is_empty())
            .collect::<Vec<&str>>();
        println!("Value: |{:?}|", value);
        if value.len() == 1 {
            return Err(ReadError::IncorrectData(format!("Missing {} value", field)));
        };
        if value.len() > 2 {
            return Err(ReadError::IncorrectData(
                "Excessive separators presented".into(),
            ));
        }
        Ok(value[1].to_owned())
    }

    fn parse_tx_id(value: &str) -> Result<TxId, ReadError> {
        let tx_id = value.trim().parse::<u64>()?;
        Ok(TxId(tx_id))
    }

    fn parse_tx_type(value: &str) -> Result<TxType, ReadError> {
        match value.trim() {
            "DEPOSIT" => Ok(TxType::Deposit),
            "TRANSFER" => Ok(TxType::Transfer),
            "WITHDRAWAL" => Ok(TxType::Withdrawal),
            _ => Err(ReadError::IncorrectData("Incorrect tx type".into())),
        }
    }

    fn parse_from_user_id(value: &str, tx_type: &TxType) -> Result<FromUserId, ReadError> {
        match tx_type {
            TxType::Deposit => Ok(FromUserId(0)),
            _ => Ok(FromUserId(value.trim().parse::<u64>()?)),
        }
    }

    fn parse_to_user_id(value: &str, tx_type: &TxType) -> Result<ToUserId, ReadError> {
        match tx_type {
            TxType::Withdrawal => Ok(ToUserId(0)),
            _ => Ok(ToUserId(value.trim().parse::<u64>()?)),
        }
    }

    fn parse_amount(value: &str) -> Result<Amount, ReadError> {
        let amount = value.trim().parse::<u64>()?;
        Ok(Amount(amount))
    }

    fn parse_timestamp(value: &str) -> Result<Timestamp, ReadError> {
        let timestamp = value.trim().parse::<u64>()?;
        Ok(Timestamp(timestamp))
    }

    fn parse_status(value: &str) -> Result<Status, ReadError> {
        match value.trim() {
            "SUCCESS" => Ok(Status::Success),
            "FAILURE" => Ok(Status::Failure),
            "PENDING" => Ok(Status::Pending),
            _ => Err(ReadError::IncorrectData("Incorrect status".into())),
        }
    }

    fn parse_description(value: &str) -> Description {
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

mod tests {
    use crate::ReadError;
    #[cfg(test)]
    use crate::{
        Amount, Body, DescLen, Description, FromUserId, Status, Timestamp, ToUserId, TxId, TxType,
        txt::YPBankTxtRecord,
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
            Description(Some("oleg".as_bytes().to_vec())),
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
    fn create_txt_record(
        txtype: TxType,
        status: Status,
        size: Option<u32>,
        with_desc: bool,
    ) -> YPBankTxtRecord {
        YPBankTxtRecord {
            body: if with_desc {
                create_body_with_desc(txtype, status, size)
            } else {
                create_body_without_desc(txtype, status)
            },
        }
    }

    #[test]
    fn test_write_description_with_data() {
        let record = create_txt_record(TxType::Deposit, Status::Failure, None, true);
        assert!(record.write_description().is_ok_and(|v| v.contains("oleg")));
    }

    #[test]
    fn test_write_description_without_data() {
        let record = create_txt_record(TxType::Deposit, Status::Failure, None, false);
        assert!(record.write_description().is_ok_and(|v| v.is_empty()));
    }

    #[test]
    fn test_write_description_broken_utf8() {
        let mut record = create_txt_record(TxType::Deposit, Status::Failure, None, true);
        record.body.8.0 = Some([0xFF, 0xFF].as_slice().to_vec());
        assert!(record.write_description().is_err());
    }

    #[test]
    fn test_parse_description_empty() {
        assert!(YPBankTxtRecord::parse_description("").0.is_none());
    }

    #[test]
    fn test_parse_description_with_data() {
        assert!(
            YPBankTxtRecord::parse_description("oleg")
                .0
                .is_some_and(|v| v == "oleg".as_bytes().to_vec())
        );
    }

    #[test]
    fn test_parse_status_success() {
        assert!(YPBankTxtRecord::parse_status("SUCCESS").is_ok_and(|v| v == Status::Success));
        assert!(YPBankTxtRecord::parse_status("FAILURE").is_ok_and(|v| v == Status::Failure));
        assert!(YPBankTxtRecord::parse_status("PENDING").is_ok_and(|v| v == Status::Pending));
    }

    #[test]
    fn test_parse_status_failed() {
        assert!(YPBankTxtRecord::parse_status("oleg").is_err());
    }

    #[test]
    fn test_parse_timestamp_failed() {
        assert!(YPBankTxtRecord::parse_timestamp("-9").is_err());
        assert!(YPBankTxtRecord::parse_timestamp("oleg").is_err());
    }

    #[test]
    fn test_parse_timestamp_success() {
        assert!(YPBankTxtRecord::parse_timestamp("24").is_ok());
    }

    #[test]
    fn test_parse_amount_failed() {
        assert!(YPBankTxtRecord::parse_amount("-9").is_err());
        assert!(YPBankTxtRecord::parse_amount("oleg").is_err());
    }

    #[test]
    fn test_parse_amount_success() {
        assert!(YPBankTxtRecord::parse_amount("24").is_ok());
    }

    #[test]
    fn test_parse_from_user_id_failed() {
        let tx_type = TxType::Withdrawal;
        assert!(YPBankTxtRecord::parse_from_user_id("field", &tx_type).is_err());
        assert!(YPBankTxtRecord::parse_from_user_id("22.22", &tx_type).is_err());
    }

    #[test]
    fn test_parse_from_user_id_success() {
        let tx_type = TxType::Deposit;
        assert!(YPBankTxtRecord::parse_from_user_id("11", &tx_type).is_ok_and(|v| v.0 == 0));
        let tx_type = TxType::Transfer;
        assert!(YPBankTxtRecord::parse_from_user_id("11", &tx_type).is_ok_and(|v| v.0 == 11));
        let tx_type = TxType::Withdrawal;
        assert!(YPBankTxtRecord::parse_from_user_id("11", &tx_type).is_ok_and(|v| v.0 == 11));
    }

    #[test]
    fn test_parse_to_user_id_failed() {
        let tx_type = TxType::Deposit;
        assert!(YPBankTxtRecord::parse_to_user_id("field", &tx_type).is_err());
        assert!(YPBankTxtRecord::parse_to_user_id("22.22", &tx_type).is_err());
    }

    #[test]
    fn test_parse_to_user_id_success() {
        let tx_type = TxType::Withdrawal;
        assert!(YPBankTxtRecord::parse_to_user_id("11", &tx_type).is_ok_and(|v| v.0 == 0));
        let tx_type = TxType::Transfer;
        assert!(YPBankTxtRecord::parse_to_user_id("11", &tx_type).is_ok_and(|v| v.0 == 11));
        let tx_type = TxType::Deposit;
        assert!(YPBankTxtRecord::parse_to_user_id("11", &tx_type).is_ok_and(|v| v.0 == 11));
    }

    #[test]
    fn test_parse_tx_id_success() {
        assert!(YPBankTxtRecord::parse_tx_id("11").is_ok());
    }

    #[test]
    fn test_parse_tx_id_failed() {
        assert!(YPBankTxtRecord::parse_tx_id("-11").is_err());
        assert!(YPBankTxtRecord::parse_tx_id("22.1").is_err());
        assert!(YPBankTxtRecord::parse_tx_id("oleg").is_err());
    }

    #[test]
    fn test_parse_tx_type_success() {
        assert!(matches!(
            YPBankTxtRecord::parse_tx_type("DEPOSIT"),
            Ok(TxType::Deposit)
        ));
        assert!(matches!(
            YPBankTxtRecord::parse_tx_type("TRANSFER"),
            Ok(TxType::Transfer)
        ));
        assert!(matches!(
            YPBankTxtRecord::parse_tx_type("WITHDRAWAL"),
            Ok(TxType::Withdrawal)
        ));
    }

    #[test]
    fn test_parse_tx_type_failed() {
        assert!(YPBankTxtRecord::parse_tx_type("oleg").is_err());
    }

    #[test]
    fn test_build_field_value_missing_field() {
        let result = YPBankTxtRecord::build_field_value("oleg: oleg", "not oleg");
        assert!(result.is_err_and(|e| e.to_string() == "Missing not oleg field"))
    }

    #[test]
    fn test_build_field_value_missing_value() {
        let result = YPBankTxtRecord::build_field_value("oleg: ", "oleg");
        assert!(result.is_err_and(|e| e.to_string() == "Missing oleg value"))
    }

    #[test]
    fn test_build_field_value_excessive_separators() {
        let result = YPBankTxtRecord::build_field_value("oleg: asd: asd", "oleg");
        assert!(result.is_err_and(|e| e.to_string() == "Excessive separators presented"))
    }

    #[test]
    fn test_build_filed_value_success() {
        let result = YPBankTxtRecord::build_field_value("oleg: is here", "oleg");
        assert!(result.is_ok_and(|v| v == "is here"))
    }

    #[test]
    fn test_parse_single() {
        let text = "TX_ID: 1000000000000000
        TX_TYPE: DEPOSIT
        FROM_USER_ID: 0
        TO_USER_ID: 9223372036854775807
        AMOUNT: 100
        TIMESTAMP: 1633036860000
        STATUS: FAILURE
        DESCRIPTION: \"Record number 1\"";
        assert!(YPBankTxtRecord::parse(text.as_bytes()).is_ok_and(|v| v.len() == 1));
    }

    #[test]
    fn test_parse_multiple() {
        let text = "TX_ID: 1000000000000000
        TX_TYPE: DEPOSIT
        FROM_USER_ID: 0
        TO_USER_ID: 9223372036854775807
        AMOUNT: 100
        TIMESTAMP: 1633036860000
        STATUS: FAILURE
        DESCRIPTION: \"Record number 1\"

        TX_ID: 1000000000000000
        TX_TYPE: DEPOSIT
        FROM_USER_ID: 0
        TO_USER_ID: 9223372036854775807
        AMOUNT: 100
        TIMESTAMP: 1633036860000
        STATUS: FAILURE
        DESCRIPTION: \"Record number 1\"

        ";
        assert!(YPBankTxtRecord::parse(text.as_bytes()).is_ok_and(|v| v.len() == 2));
    }

    #[test]
    fn test_parse_no_separator() {
        let text = "TX_ID: 1000000000000000
        TX_TYPE: DEPOSIT
        FROM_USER_ID 0
        TO_USER_ID: 9223372036854775807
        AMOUNT: 100
        TIMESTAMP: 1633036860000
        STATUS: FAILURE
        DESCRIPTION: \"Record number 1\"";
        assert!(
            YPBankTxtRecord::parse(text.as_bytes())
                .is_err_and(|e| e.to_string() == "Missing separator")
        )
    }

    #[test]
    fn test_parse_unknown_field() {
        let text = "TX_ID: 1000000000000000
        TX_TYPE: DEPOSIT
        FROM_USER_ID: 0
        TO_USER_ID: 9223372036854775807
        OLEG: 100
        TIMESTAMP: 1633036860000
        STATUS: FAILURE
        DESCRIPTION: \"Record number 1\"";
        assert!(
            YPBankTxtRecord::parse(text.as_bytes())
                .is_err_and(|e| e.to_string() == "Unsupported field")
        )
    }
}
