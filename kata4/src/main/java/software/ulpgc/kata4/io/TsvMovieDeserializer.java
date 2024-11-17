package software.ulpgc.kata4.io;

import software.ulpgc.kata4.model.Movie;

public class TsvMovieDeserializer implements MovieDeserializer{
    private static final int ID = 0;
    private static final int TITLE = 3;
    private static final int YEAR = 5;
    private static final int DURATION = 7;

    @Override
    public Movie deserialize(String line) {
        return read(line.split("\t"));
    }

    private Movie read(String[] fields) {
        return new Movie(
                fields[ID],
                fields[TITLE],
                toInteger(fields[YEAR]),
                toInteger(fields[DURATION])
        );
    }

    private int toInteger(String field) {
        try {
            return Integer.parseInt(field);
        }
        catch (NumberFormatException e) {
            return -1;
        }
    }
}
