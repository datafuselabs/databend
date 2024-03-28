use arrow_udf_wasm::build::*;
use databend_common_base::base::tokio;
use databend_common_compress::CompressAlgorithm;
use databend_common_compress::CompressCodec;
use databend_common_exception::Result;
use databend_common_storage::DataOperator;
use databend_enterprise_query::test_kits::context::EESetup;
use databend_query::test_kits::*;
use opendal::Operator;
use tokio::time::sleep;
use tokio::time::Duration;
use uuid::Uuid;

#[tokio::test(flavor = "multi_thread")]
async fn test_udf_js_gcd() -> Result<()> {
    let fixture = TestFixture::setup_with_custom(EESetup::new()).await?;

    // Create table
    fixture
        .execute_command_v2(r#"CREATE FUNCTION gcd (INT, INT) RETURNS BIGINT LANGUAGE javascript HANDLER = 'gcd_js' AS $$
                export function gcd_js(a, b) {
                    while (b != 0) {
                        let t = b;
                        b = a % b;
                        a = t;
                    }
                    return a;
                }
            $$
            "#
        ).await?;

    let res = fixture
        .execute_command_v2(
            r#"
            select 
                number, gcd(number * 3, number * 6) 
            from 
                numbers(5) 
            where number > 0 order by 1;        
            "#,
        )
        .await?;

    log::info!("Response {:#?}", res);

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_udf_py_gcd() -> Result<()> {
    let fixture = TestFixture::setup_with_custom(EESetup::new()).await?;

    let py_gcd_function = r#"
def gcd_py(a, b):
    while b != 0:
        a, b = b, a % b
    return a
    "#;

    // Create table
    fixture
        .execute_command_v2(
            &format!(r#"CREATE FUNCTION gcd (INT, INT) RETURNS BIGINT LANGUAGE python HANDLER = 'gcd_py' AS $${}$$"#,
            py_gcd_function)
        ).await?;

    let res = fixture
        .execute_command_v2(
            r#"
            select 
                number, gcd(number * 3, number * 6) 
            from 
                numbers(5) 
            where number > 0 order by 1;        
            "#,
        )
        .await?;

    log::info!("Response {:#?}", res);

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_udf_wasm_gcd() -> Result<()> {
    let fixture = TestFixture::setup_with_custom(EESetup::new()).await?;

    let manifest = r#"
        [dependencies]
        chrono = "0.4"
        "#;

    let script = r#"
        use arrow_udf::function;
        
        #[function("wasm_gcd(int, int)->int")]
        fn wasm_gcd(mut a: i32, mut b: i32) -> i32 {
            while b != 0 {
                (a, b) = (b, a % b);
            }
            a
        }
        "#;

    log::info!("Building WASM blob...");

    let code_blob = build(manifest, script).unwrap();

    let mut encoder = CompressCodec::from(CompressAlgorithm::Zstd);
    let compressed = encoder.compress_all(&code_blob)?;

    let operator = DataOperator::instance().operator();

    let wasm_module_path = format!("test_dir/{}.wasm", Uuid::new_v4().to_string());

    operator.write(&wasm_module_path, compressed).await?;

    log::info!("WASM blob compression successful: {}", wasm_module_path);

    let command = format!(
        r#"CREATE FUNCTION wasm_gcd (INT, INT) RETURNS BIGINT LANGUAGE wasm HANDLER = 'wasm_gcd(int4,int4)->int4' AS $${}$$"#,
        wasm_module_path
    );

    log::info!("Creating UDF: {}", command);

    // Create UDF function
    fixture.execute_command_v2(&command).await?;

    log::info!("UDF created successfully");

    log::info!("Executing SQL command:");

    let res = fixture
        .execute_command_v2(
            r#"
            select
                number, wasm_gcd(number * 3, number * 6)
            from
                numbers(5)
            where number > 0 order by 1;
            "#,
        )
        .await?;

    log::info!("SQL command executed successfully");

    log::info!("Response {:#?}", res);

    Ok(())
}
