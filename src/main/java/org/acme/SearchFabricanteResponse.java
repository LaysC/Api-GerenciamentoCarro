package org.acme;

import java.util.ArrayList;
import java.util.List;

public class SearchFabricanteResponse {
    public List<Fabricante> Fabricantes = new ArrayList<>();

    public long TotalFabricantes;
    public int TotalPages;
    public boolean HasMore;
    public String NextPage;
}