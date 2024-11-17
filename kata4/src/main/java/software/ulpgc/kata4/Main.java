package software.ulpgc.kata4;

import software.ulpgc.kata4.commands.ImportCommand;

public class Main {
    public static void main(String[] args) throws Exception {
        ImportCommand command = new ImportCommand();
        command.execute();
    }

}
