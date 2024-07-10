use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize)]
pub struct KV {
    pub key: String,
    pub value: String,
}

#[derive(Deserialize, Serialize)]
pub struct Key {
    pub key: String,
}

#[derive(Deserialize, Serialize)]
pub struct Token {
    pub token: String,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct User {
    pub username: String,
    pub password: String,
}

#[derive(Deserialize, Serialize)]
pub struct UserName {
    pub name: String,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct ID {
    pub id: String,
}
