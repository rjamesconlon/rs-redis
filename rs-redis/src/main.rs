use rs_redis::cli;

fn main() {
    println!("USING COMMAND SET NAME \"JOHN DOE\"");
    println!("{}", cli::read_cli_input("SET NAME \"JOHN DOE\"").unwrap());
    println!("USING COMMAND GET NAME \"JOHN DOE\"");
    println!("{}", cli::read_cli_input("GET NAME").unwrap());
}
