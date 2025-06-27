use rs_redis::cli;

fn main() {
    println!("{:?}", cli::read_cli_input("ECHO \"HELLO WORLD\""));
}
