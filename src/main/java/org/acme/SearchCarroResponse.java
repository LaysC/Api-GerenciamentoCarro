package org.acme;

import java.util.ArrayList;
import java.util.List;

public class SearchCarroResponse {
    public List<Carro> Carros = new ArrayList<>();

    public long TotalCarros;
    public int TotalPages;
    public boolean HasMore;
    public String NextPage;
}