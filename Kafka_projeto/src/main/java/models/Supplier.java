package models;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.*;

@NoArgsConstructor
@AllArgsConstructor
@Getter
@Setter
@ToString
@JsonIgnoreProperties(ignoreUnknown = true)
public class Supplier {
    @JsonProperty("id")
    private String supplierId;

    @JsonProperty("nome")
    private String name;

    @JsonProperty("localizacao")
    private String location;
}
