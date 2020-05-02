package pl.fermich.lab;

public class App {
    public String getGreeting() {
        return "Hello world.";
    }

    public static void main(String[] args) {
        RegisterTask registerTask = new RegisterTask();
        registerTask.register();
    }
}
