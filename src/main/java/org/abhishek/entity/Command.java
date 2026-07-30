package org.abhishek.entity;

public record Command(Cmd cmd, String key, String value)
{
    public Command(String key, String value)
    {
        this(Cmd.PUT, key, value);
    }
    public Command(String key)
    {
        this(Cmd.DELETE, key, null);
    }

}
