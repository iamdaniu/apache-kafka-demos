package de.predic8.m_toggle;

public class Toggle {
    @Override public String toString() {
        return "Toggle {" +
                "name='" + name + '\'' +
                ", description='" + description + '\'' +
                ", enabled=" + enabled +
                '}';
    }

    private String name;

    private String description;
    private boolean enabled;

    public Toggle() {}

    public Toggle(String name, String description, boolean enabled) {
        this.name = name;
        this.description = description;
        this.enabled = enabled;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean newState) {
        enabled = newState;
     }
}
